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
rm -r /home/sweet/tmp/sand/volumn2/data
rm /home/sweet/tmp/sand/volumn2/volumn.log
rm /home/sweet/tmp/sand/volumn2/config
rm -r /home/sweet/tmp/sand/volumn3/data
rm /home/sweet/tmp/sand/volumn3/volumn.log
rm /home/sweet/tmp/sand/volumn3/config

cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn1/volumn.py
cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn2/volumn.py
cp /home/sweet/DFS/server/volumn/volumn.py /home/sweet/tmp/sand/volumn3/volumn.py
