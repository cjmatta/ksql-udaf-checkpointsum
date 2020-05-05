# ksql-udaf-checkpointsum
A udaf for ksqlDB that computes SUM of a stream of records where some are the absolute value, and some are the delta. Delta values are added to the current aggregation value, while absolute values are set and passed on. 

### Example
Given a stream of events below, this function correctly outputs  values based on type. 
```$xslt
t1:{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 10}
t2:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t3:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t4:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t5:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t6:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}
t7:{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 20}
t8:{"account": "AAA", "security": "xyz", "type": "delta", "amount": 1}
t9:{"account": "AAA", "security": "zzz", "type": "absolute", "amount": 2}
t9:{"account": "AAA", "security": "zzz", "type": "delta", "amount": 1}

```

at t3 above:
```$xslt
{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 10}
{"account": "AAA", "security": "zzz", "type": "delta", "amount": 3}
```

at t9 above:
```$xslt
{"account": "AAA", "security": "xyz", "type": "absolute", "amount": 21}
{"account": "AAA", "security": "zzz", "type": "delta", "amount": 7}
```

### Important

The data structure this UDAF expects is a `STRUCT(type VARCHAR, amount DOUBLE)`

The `amount` column only expects the following two strings: "delta" or "absolute", so if there's a stream of events:
```$xslt
{"account": "AAA", "type": "delta", "amount": 1}
{"account": "AAA", "type": "delta", "amount": 1}
{"account": "AAA", "type": "state", "amount": 20}
{"account": "AAA", "type": "delta", "amount": 1}
```

The `type` value "state" would need to be converted to "absolute":
```SQL
create table accountsum as
    SELECT account, checkpoint_sum(
      STRUCT(type := CASE WHEN type = 'state' THEN 'absolute' ELSE 'delta' end, value := amount)
    ) as amount
    from accountstream group by account;
```


### WARNINGS !!!
This UDAF assumes all records are in correct order, out of order records will provide incorrect results. 

This UDAF doesn't support windowed aggregations