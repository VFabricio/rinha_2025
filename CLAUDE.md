# Rinha

## Introduction

This repo is part of Rinha de Backend, a competition aimed at constructing efficient backend services.

# Problem Setup

In this edition, our goal is to build a gateway that forwards payment requests to one of two payment processors, called de `default` and `fallback` processors.
Every time the gateway receives a payment (which can be assumed to all have the same value, but this value will be determined at run time) it can freely choose which processor to send it.
However, each processor charges a fee for processing the payment.
The exact fees will only be known at runtime, but it is guaranteed that the fee of the default processor will be smaller than the fee of the fallback one.
However, each of the processors can randomly reject requests or have random delays in processing requests.
Therefore, sending all the payments to the cheapest processor may not be optimal.
As will be detailed later, the processor have endpoints that report on their current status, so that we have at least some visibility of how deteriorated their one.
However, these endpoints are heavily rate limited.
So, it is not practical, for example, to check the processor statuses before sending each payment.
The gateway must also keep track of how many payments and of the total amounts it sent to each processor.

# Evaluation

The final goal is to maximize profit for the gateway.
The gateway revenue is given the total amount it processed.
However, the gateway has an expense: the total fees charged by the processors.
In addition, during evaluation, it will be checked several times if the number of payments and total amounts claimed by the gateway match the ones reported by the processor.
If there is ever an inconsistency, a 35% fine on total revenue will be levied.
Finally, there will be a bonus if the gateway is fast enough, measured by the p99 of all requests sent.
The bonus rate, which will then be applied on top of revenue is max((11 - p99) * 0.02, 0), where p99 is rounded to milisecond accuracy.
p99s of less than 1 ms will be counted as 1 ms.
So, for example, a p99 larger than 10ms will be neither rewarded nor punished.
However, a 10ms p99 will lead to a 2% bonus.
But the rewards then increase by 2% for each extra ms of improvement, up to a 20% bonus for 1ms.

# Architectural Restrictions

The services must be provided as a docker-compose file.
The payment processors have already been created by the competition organizers and must not be modified.
We should use their already published docker images in our compose file.
We have to implement the gateway.
It must not however, be exposed externally directly.
It is mandatory to have at least instances of the gateway, with a load balancer in front of them.
During evaluation, the request addressed to the gateway will be sent to the load balancer.
The load balancer is expected to respond to HTTP requests.
It can choose whether it uses HTTP/1.1 or HTTP/2.
There is no restriction in how the load balancer communicates with the gateway instances.
It is allowed to use any other services we judge necessary, by pulling them to the compose file, as long as their docker images are published in public repositories.
Finally there are heavy resource constraints.
The compose file must limit the total CPUs to 1.5 and the total memory to 350 MB.
We are allowed to divide these resources between our services as we choose, as long as the total amount limits are not exceeded.

# Strategy

We will write everything in Rust.
We will focus on performance, even it means schewing some good practices.
If we include external libraries, they must be very lightweight.
We must be open to writing things ourselves if we believe that will improve performance.
We may have some observability, mainly in the form of print statements, but that will need to be stripped out in the production build.
The evaluation will be run on a Debian 6.1.140-1 machine.
We may build an optimized binary for it if we judge it helpful.

# Specifications

## Gateway

### Payments

The gateway will receive the payments as POST request to /payments, with a payload like
```json
{
    "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
    "amount": 19.90
}
```
If the request is successfull, an HTTP status between 200 and 204 must be returned.
Everything else will be interpreted as a failed request.
However, we are allowed to report success even if the payment has only been enqued to be processed later and has not been successfully acknowledged by the payment processors.
These discrepancies will be noted in the final results but not penalized.
If, however, the total amount of successes we report turns out to be lower than the sum of the successfull payments reported by the processors, we will be disqualified.
The request body can be anything and will be ignored.

### Payments Summary

The gateway must be able to, at any time, report a summary the payments processed between any two times.
It will receive a GET request to /payment-summary, containing query parameters `from` and `to`, containing ISO timestamps, like, for example GET /payments-summary?from=2020-07-10T12:34:56.000Z&to=2020-07-10T12:35:56.000Z.
The response must be a 200 OK with a body like
```json
{
    "default" : {
        "totalRequests": 43236,
        "totalAmount": 415542345.98
    },
    "fallback" : {
        "totalRequests": 423545,
        "totalAmount": 329347.34
    }
}
```

## Payment Processor

### Payments

The payment processor will receive the payments as POST request to /payments, with a payload like
```json
{
    "correlationId": "4a7901b8-7d26-4d9d-aa19-4dc1c7cf60b3",
    "amount": 19.90,
    "requestedAt" : "2025-07-15T12:34:56.000Z"
}
```
A successfull response will be a 200 OK with body
```json
{
    "message": "payment processed successfully"
}
```
Remember that, when the server is stressed it may respond with 5xx failures as well.

## Health-Check

We can send a GET to /payments/service-health, which the server is expected to respond with an HTTP 200 status and a body like
```json
{
    "failing": false,
    "minResponseTime": 100
}
```
If `failing` is true, requests to /payments will currently be returning 5xx errors.
`minResponseTime` is the _minimum_ response time for requests to /payments.
It is unspecified whether the health check point itself can be unavailable or delayed.
Crucially, request to this endpoint are to limited to 1 in each 5 s.
If this limit is not respected, the processor will return an HTTP 429 status.

# AI Instructions

Do not include explanatory comments in the code.
Do not attempt to generate the project all at once.
We will build this gradually.
My intention is to slowly guide through each section.
If you are ever unsure about the best direction, ask question first before starting to write code.
