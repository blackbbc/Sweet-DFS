# -*- coding: utf-8 -*-

import time
import _thread

import client

def upload(start, end):
    for i in range(start, end):
        client.upload('inputs/%d' % i)
        print(i)

def main():
    _thread.start_new_thread(upload, (1, 101))
    _thread.start_new_thread(upload, (101, 201))
    _thread.start_new_thread(upload, (201, 301))
    _thread.start_new_thread(upload, (301, 401))
    _thread.start_new_thread(upload, (401, 501))

    while True:
        time.sleep(1)

if __name__ == '__main__':
    main()
    upload(1, 101)
