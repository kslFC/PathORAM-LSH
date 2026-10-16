*A simple implementation of Path ORAM without encryption, paper "Path ORAM: An Extremely Simple Oblivious RAM Protocol", proposed by Emil Stefanov et al.

*need to create a new directory "serverStorage" in /PathORAM to store bucket file.

*need jar package guava-19.0.jar.

*first run class "Server" in package com.server, then run class "Client" in package com.client.

** This project has been forked from 
https://github.com/caohuikang/PathORAM

## READ experiment

The experiment loads `data/0.bin` through `data/47.bin` as 48 logical blocks. Each block is 1,392 bytes. The Path ORAM tree has 31 buckets, with four slots per bucket.

The client first writes all 48 files, then performs 300 reads. Block IDs are split into three contiguous groups of 16. Access starts in group 0; on each read it switches to a different group with probability 0.20, then uniformly selects a block from the current group.

Run the cache and no-cache conditions with the same access seed and transition probability to replay the same request sequence, for example `baseline-cache 12345 0.20` and `baseline-no-cache 12345 0.20`. The default transition probability is 0.20.


