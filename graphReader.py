import numpy as np
import Pycluster as pc

def makeGraph (filename):
	node_list = []
	edge_list = []
	edge = []
	count_nodes = 0
	ended = True
	
	f=open(filename,"rb")
	while True:
		x = f.readline()
		if not x:
			break
		x = x.split(" ")
		


		if(x[1] == 'node'):
			node_list.append(int(x[2].split(',')[0]))
			count_nodes +=1
			
		if(x[1] == 'edge' and ended==True):
			ended = False
			adjMat = np.zeros((max(node_list)+1,max(node_list)+1))
			
		if(x[1] == 'edge'):
			a = x[2].split('-')
			edge = [int(a[0]),int(a[1].strip())]
			if(edge[0] == edge[1]):
				continue
			edge_list.append(edge)
			adjMat[(edge[0])][(edge[1])] = 1
	f.close()
	
	for i in range(max(node_list)):
		if(i in node_list):
			adjMat[i][i] = 1;
	
	return (node_list,edge_list, adjMat, count_nodes)
	
(nodes,edges,adjMat, count) = (makeGraph("log-graph.out"))
print count
print max(nodes)
errorOld = 5000
index = 175

'''for i in range(max(nodes)+1):
	clusterid,error,nfound = pc.kcluster(adjMat, nclusters=i+1, transpose=0,npass=1,method='a',dist='e')
	if (i==0):
		errorOld = error
	elif (error == 0):
		index = i
		break
	elif(error < errorOld):
		index = i
		errorOld = error'''
		
clusterid,error,nfound = pc.kcluster(adjMat, nclusters=index+1, transpose=0,npass=1,method='a',dist='e')
	
print(clusterid)
print nfound
print error

neglect = set([])

for i in range (max(nodes)+1):
	if (sum(adjMat[i]) == 0.0):
		neglect.add(clusterid[i])



print neglect

f=open("output.txt","wb")
for i in range((max(nodes))+1):
	if not (sum(adjMat[i]) == 0.0):
		f.write("node " + str(i) + " : Cluster " + str(clusterid[i]) + "\n")
f.close()


