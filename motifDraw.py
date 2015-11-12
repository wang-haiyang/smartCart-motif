# -*- encoding: utf-8

import matplotlib 
matplotlib.use('Agg')   
import matplotlib.pyplot as plt
from matplotlib.text import Text
from matplotlib.patches import FancyArrowPatch, Circle
zhfont1 = matplotlib.font_manager.FontProperties(fname='/usr/share/fonts/chinese/TrueType/FZCSJW.TTF')
import networkx as nx

def draw_network(G, pos, ax):
    """ Draw network with curved edges.
    """
    for n in G:
        c = Circle(pos[n], radius=0.05, alpha=0.7, color='red', gid='11')
        ax.add_patch(c)
        G.node[n]['patch'] = c
        ax.text(pos[n][0]-0.01, pos[n][1]-0.015, str(n+1))
    seen={}
    for (u,v,d) in G.edges(data=True):
        n1 = G.node[u]['patch']
        n2 = G.node[v]['patch']
        rad = 0.1
        if (u,v) in seen:
            rad = seen.get((u,v))
            rad = (rad + np.sign(rad) * 0.1) * -1
        alpha = 0.5; color = 'k'
        e = FancyArrowPatch(n1.center, n2.center,
                            patchA=n1, patchB=n2,
                            arrowstyle='->',
                            connectionstyle='arc3,rad=%s' % rad,
                            mutation_scale=10.0,
                            lw=2, alpha=alpha, color=color)
        seen[(u, v)] = rad
        ax.add_patch(e)

def location_to_motif(locations):
	locations = locations.split(',')
	dateFlag = ''
	motifList = []
	for i in xrange(0,len(locations)):
		_date = locations[i].split(' ')[0]
		_time = locations[i].split(' ')[1]
		station = locations[i].split(' ')[2].split('号线')[1]
		if locations[i].split(' ')[3]!='':
			fare = float(locations[i].split(' ')[3])
		else:
			fare = float(locations[i].split(' ')[4])


		if dateFlag != _date:#if another day, initialize
			motifList.append({})
			nodeDict = {}
		if not nodeDict.has_key(station):#map station name to node num
			nodeDict[station] = len(nodeDict)
		if not motifList[-1].has_key(nodeDict[station]):#add the appeared node to today's motif graph
			motifList[-1][nodeDict[station]] = []
		if fare==0:#if enter a station, add a directed edge to next station(if it's an exit)
			if (i+1)<len(locations):
				if locations[i+1].split(' ')[0]==_date:#yet is today's
					if locations[i+1].split(' ')[3]!='':
						n_fare = float(locations[i+1].split(' ')[3])
					else:
						n_fare = float(locations[i+1].split(' ')[4])
					if float(n_fare)!=0:#exit from next station
						n_station = locations[i+1].split(' ')[2].split('号线')[1]
						if not nodeDict.has_key(n_station):
							nodeDict[n_station] = len(nodeDict)
						motifList[-1][nodeDict[station]].append(nodeDict[n_station])
		dateFlag = _date

	return motifList

def style_conv(list_dict):
	dicts = str(list_dict).strip('[]')
	dicts = dicts.split(', {')
	rsl = []
	for i in xrange(0, len(dicts)):
		if i ==0:
			rsl.append(dicts[i])
		else:
			rsl.append('{' + dicts[i])
	return rsl

motifs = [{0: [1], 1: [0]},{0: [1], 1: []},{0: [1], 1: [2], 2: []},{0: [1], 1: [], 2: [0]}
,{0: [1], 1: [2], 2: [0]},{0: [1], 1: [], 2: [3], 3: []},{0: [1], 1: [2], 2: [3], 3: []}
,{0: [1], 1: [2], 2: [], 3: [0]},{0: [1], 1: [], 2: [3], 3: [0]},{0: [1], 1: [2, 0], 2: [1]}]

for i in xrange(0, len(motifs)):
	plt.clf()
	DG=nx.DiGraph(motifs[i])
	ax=plt.gca()
	pos=nx.spring_layout(DG)
	draw_network(DG,pos,ax)
	ax.autoscale()
	plt.axis('equal')
	plt.axis('off')
	plt.savefig('figs/mofit' + str(i+1) + '.png')