#a deployment script for our ML cluster of 4 machines
trap ''  HUP   # script will ignore HANGUP signal

for i in {0..3}; do ssh ML$i "pkill -KILL java; pkill rmiregistry; rm /home/mluser/simplemr/*.log; rm /home/mluser/simplemr/*.out; rm -rf /home/mluser/simplemr/TEMP_DIR/*; rm -rf /home/mluser/simplemr/temp_dir/*; rm -rf /home/mluser/simplemr/data_dir/*;" ; done

for i in {1..3}; do scp -r /home/mluser/simplemr/ ML$i: ; done

#for i in {0..3}; do ssh ML$i "cd /home/mluser/simplemr/; mvn package" ; done
cd /home/mluser/simplemr/; mvn package
for i in {1..3}; do scp -r /home/mluser/simplemr/dist ML$i:simplemr/ ; done

#===At all workers
for i in {0..3}; do echo "-----ML$i-----"; ssh ML$i "cd /home/mluser/simplemr; nohup dist/bin/registry 7777  > /dev/null 2>&1 &" ; done
	

#===At master
	
for i in {0..0}; do echo "-----ML$i-----"; ssh ML$i "cd /home/mluser/simplemr; nohup dist/bin/dfs-master -l dfsmaster.log -rp 7777 > /dev/null 2>&1 &" ; done

#===at slaves
for i in {0..3}; do echo "-----ML$i-----"; ssh ML$i "cd /home/mluser/simplemr; nohup  dist/bin/dfs-slave -d data_dir -mh 10.68.213.32 -mp 7777 -rp 7777 -n ML$i > /dev/null 2>&1 &" ; done


#===at master
for i in {0..0}; do echo "-----ML$i-----"; ssh ML$i "cd /home/mluser/simplemr; nohup dist/bin/mapreduce-jobtracker -dh 10.68.213.32 -dp 7777 -rp 7777 -fp 8888 -t temp_dir  > /dev/null 2>&1 &" ; done


#===at slaves
for i in {0..3}; do echo "-----ML$i-----"; ssh ML$i "cd /home/mluser/simplemr; nohup dist/bin/mapreduce-tasktracker -dh 10.68.213.32 -dp 7777 -jh 10.68.213.32 -jp 7777 -rp 7777 -fp 8889 -t TEMP_DIR > /dev/null 2>&1 &" ; done

#initial_port=8889
#for i in {0..3}; do echo "-----ML$i-----"; for j in {0..3}; do echo "on port# $initial_port"; ssh ML$i "cd /home/mluser/simplemr; nohup dist/bin/mapreduce-tasktracker -dh 10.68.213.32 -dp 7777 -jh 10.68.213.32 -jp 7777 -rp 7777 -fp $initial_port -t TEMP_DIR > /dev/null 2>&1 &";  initial_port=$((initial_port+1)) ; done; done

