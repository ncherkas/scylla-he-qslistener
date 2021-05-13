# scylla-he-qslistener
Scylla DB Home Exercise

## Running a demo test
As simple as:
```
go test
```

## Using as a library
```go
...
qsListener := qslistener.New(listener)
qsListener.SetLimitPerConn(1024 * 1024)
qsListener.SetLimitGlobal(5 * 1024 * 1024)
qsListener.EnableThroughputLogging()
...
```

## Some comments and considerations from my side
1st thank you for giving me a chance and sending this exercise. I did really enjoy working on it even though it was a bit challenging one for me.
Now, some notes from me:
 - the solution also includes `loggingConn` which helps to verify the throttled throughput
 - one of the challenges I had was the actual `golang.org/x/time` Limiter library, and its limitations e.g. forcing you to set burst if you want to use `WaitN()` API 
 - in this solution I tried to use Go paradigm about avoiding the shared memory, overall good but in some places (e.g. `loggingConn`) may seem to feel like a bit over-engineered
 - due to lack of time, didn't plugged in any logger
 - also, due to a limited time that I could spend, didn't cover it properly with unit tests, sorry for that
 - among other things that I'd add, probably a docker-compose to have even better test and demo experience
Hope you enjoy reviewing it, cheers!
