#########################################################################
# File Name: run.sh
# Author: ma6174
# mail: ma6174@163.com
# Created Time: Tue Feb 11 14:13:14 2014
#########################################################################
#!/bin/bash
hadoop namenode -format
start-all.sh
hadoop dfs -mkdir src
hadoop dfs -put Lakes.txt src/Lakes.txt
hadoop dfs -mkdir query
hadoop dfs -put query.txt query/Lakes.txt


