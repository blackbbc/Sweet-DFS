# -*- coding: utf-8 -*-

for i in range(1, 100001):
    with open('inputs/%d' % i, 'w') as f:
        f.write(str(i))
    print(i)
