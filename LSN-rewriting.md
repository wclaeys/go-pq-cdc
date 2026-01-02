# Why LSN Rewriting Can't Be Performant

The fundamental problem with LSN rewriting for large transactions is:
```
┌─────────────────────────────────────────────────────────────────────┐
│                    THE BACKPRESSURE PROBLEM                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  PostgreSQL sends:  INSERT₁ INSERT₂ ... INSERT₁ₘ COMMIT             │
│                                                   ↑                 │
│                                         LSN we want to use          │
│                                                                     │
│  To rewrite LSN, we MUST:                                           │
│    1. Buffer INSERT₁ₘ until COMMIT arrives                          │
│    2. Meanwhile, INSERTs₁...₁ₘ₋₁ accumulate somewhere               │
│                                                                     │
│  If consumer is slower than producer:                               │
│    - Buffers fill up                                                │
│    - Receiver blocks                                                │
│    - Can't respond to PostgreSQL keepalives                         │
│    - PostgreSQL disconnects (timeout)                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Approaches That Don't Work:

```
Approach	                    Problem
Large buffer (2M)	            Eventually fills if consumer is slow; just delays the issue
Async sender goroutine	        Sender blocks on consumer channel → sender's input buffer fills → receiver blocks
Unbounded buffer	            Memory explosion for large transactions
Non-blocking send with drop	    Data loss - unacceptable
```

**The Only Way It Would Work:**

To implement LSN rewriting without backpressure, you'd need end-to-end non-blocking from receiver to consumer, which requires:
1. Make the consumer never block - but the consumer (DataFoundry) does I/O (database writes), which is inherently blocking
2. Unbounded in-memory queue - unacceptable memory cost for large transactions
3. Disk-backed queue - significant complexity and latency overhead

**Why the Tradeoff is Acceptable:**

Without LSN rewriting:
* ACK position = last DML's LSN (slightly before COMMIT's LSN)
* On restart: might receive the COMMIT message (which we skip)
* No data loss - idempotent upserts ensure correctness
* No replay of data - PostgreSQL tracks ACKs at message level

The LSN rewriting was an optimization for prettier ACK positions, not for correctness. The simple approach is both performant and correct.

*So:*

LSN rewriting (setting last message's LSN to COMMIT's LSN) is NOT implemented because it causes backpressure issues with very large transactions
(100K+ records).

The tradeoff is acceptable:
 - Without LSN rewriting: ACK position = last INSERT's LSN (slightly behind COMMIT)
 - On restart: might replay the COMMIT message (which we skip anyway)
 - Data integrity: preserved via idempotent upserts