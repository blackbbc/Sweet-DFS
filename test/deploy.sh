rm /home/sweet/tmp/sand/master1/master.log
rm /home/sweet/tmp/sand/master1/raft.bin
rm /home/sweet/tmp/sand/master2/master.log
rm /home/sweet/tmp/sand/master2/raft.bin
rm /home/sweet/tmp/sand/master3/master.log
rm /home/sweet/tmp/sand/master3/raft.bin
cp /home/sweet/DFS/server/master/master.py /home/sweet/tmp/sand/master1/master.py
cp /home/sweet/DFS/server/master/master.py /home/sweet/tmp/sand/master2/master.py
cp /home/sweet/DFS/server/master/master.py /home/sweet/tmp/sand/master3/master.py

rm -r /home/sweet/tmp/sand/volumn1/data
rm /home/sweet/tmp/sand/volumn1/volumn.log
rm /home/sweet/tmp/sand/volumn1/config
rm /home/sweet/tmp/sand/volumn1/fdb
rm /home/sweet/tmp/sand/volumn1/vdb
rm -r /home/sweet/tmp/sand/volumn2/data
rm /home/sweet/tmp/sand/volumn2/volumn.log
rm /home/sweet/tmp/sand/volumn2/config
rm /home/sweet/tmp/sand/volumn2/fdb
rm /home/sweet/tmp/sand/volumn2/vdb
rm -r /home/sweet/tmp/sand/volumn3/data
rm /home/sweet/tmp/sand/volumn3/volumn.log
rm /home/sweet/tmp/sand/volumn3/config
rm /home/sweet/tmp/sand/volumn3/fdb
rm /home/sweet/tmp/sand/volumn3/vdb

cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn1/volumn.py
cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn2/volumn.py
cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn3/volumn.py

rm client.py
rm client.log
rm fdb
rm outputs/client.py
rm outputs/client.log
rm outputs/fdb

cp /home/sweet/DFS/client/client.py client.py
cp /home/sweet/DFS/client/client.py outputs/client.py
cp test_download.py outputs/test_download.py
