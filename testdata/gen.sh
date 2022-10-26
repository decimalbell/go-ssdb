#!/bin/bash

gen () {
    gen_string
    gen_hash
    gen_zset
}

gen_string () {
    for i in {0..99}; do
        key=string$i
        value=value$i
	redis-cli -h localhost -p 8888 set $key $value
    done
}

gen_hash () {
    for i in {0..99}; do
        key=hash$i
        for j in {0..9}; do
            field=field$j
            value=value$j
            redis-cli -h localhost -p 8888 hset $key $field $value
        done
    done
}

gen_zset () {
    for i in {0..99}; do
        key=zset$i
        for j in {0..9}; do
            member=member$j
            redis-cli -h localhost -p 8888 zadd $key $j $member
        done
    done
}

gen
