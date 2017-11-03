#a deployment script for our cluster of 4 machines

user="ubuntu"
master="1"

trap ''  HUP   # script will ignore HANGUP signal

mvn package | tail

for i in {1..4}; 
	do 
	ssh $user@nrw$i "pkill -KILL java; pkill rmiregistry; rm ~/simplemr/*.log; rm ~/simplemr/*.out; rm -rf ~/simplemr/TEMP_DIR/*; rm -rf ~/simplemr/temp_dir/*; rm -rf ~/simplemr/data_dir/*;" ; 
	scp -rq ../simplemr $user@nrw$i:~ ;
	ssh $user@nrw$i "cd ~/simplemr/; export JAVA_HOME=/usr/lib/jvm/openjdk7-custom/jre; mvn package" | tail;
	ssh $user@nrw$i "alias dfs-cat='~/simplemr/dist/bin/dfs-cat -rh nrw$master -rp 7777'; alias dfs-load='~/simplemr/dist/bin/dfs-load -rh nrw$master -rp 7777'; alias dfs-ls='~/simplemr/dist/bin/dfs-ls -rh nrw$master -rp 7777'; alias dfs-rm='~/simplemr/dist/bin/dfs-rm -rh nrw$master -rp 7777'; alias simplemr-jobs='~/simplemr/dist/bin/mapreduce-jobs -rh nrw1 -rp 7777'; alias wordcount='~/simplemr/dist/bin/examples-wordcount -rh nrw1 -rp 7777';" | tail;
        done




#===At all workers
for i in {1..4}; 
	do 
	echo "-----nrw$i-----"; 
        ssh $user@nrw$i "cd ~/simplemr; nohup dist/bin/registry 7777  > /dev/null 2>&1 &" ; 
	done
	
#===At master
echo "-----nrw$master-----"; 
ssh $user@nrw$master "cd ~/simplemr; nohup dist/bin/dfs-master -l dfsmaster.log -rp 7777 > /dev/null 2>&1 &" ;

#===At slaves
for i in {1..4}; 
	do 
	echo "-----nrw$i-----"; 
        ssh $user@nrw$i "cd ~/simplemr; nohup  dist/bin/dfs-slave -d data_dir -mh nrw$master -mp 7777 -rp 7777 -n nrw$i > /dev/null 2>&1 &" ; 
	done

#===At master
echo "-----nrw$master-----"; 
ssh $user@nrw$master "cd ~/simplemr; nohup dist/bin/mapreduce-jobtracker -dh nrw$master -dp 7777 -rp 7777 -fp 8888 -t temp_dir  > /dev/null 2>&1 &" ;

#===At slaves
for i in {1..4}; 
	do 
	echo "-----nrw$i-----"; 
	ssh $user@nrw$i "cd ~/simplemr; nohup dist/bin/mapreduce-tasktracker -dh nrw$master -dp 7777 -jh nrw$master -jp 7777 -rp 7777 -fp 8889 -t TEMP_DIR > /dev/null 2>&1 &" ; 
	done
