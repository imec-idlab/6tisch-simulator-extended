# 6tisch-simulator-extended
====================

Contains new features for the python 6tisch-simulator 

Added changes by Esteban Municio <esteban.municio@uantwerpen.be>

The following changes have been included:
* protocols
	* 6P: A realistic 6Top Protocol is implemented. Messages are sent in both SHARED and TX cells. 
	  Using states and codes from http://tools.ietf.org/html/draft-wang-6tisch-6top-sublayer
	* RPL: A realistic RPL is implemented. RPL DIOs messages are sent in SHARED cells. A simple Trickle alogirthm is used
* phy layer
	* Rayleigh model has been added (Friis + Rayleigh) for NLOS scenarios
	* Variable RSSI at every cycle. 
	* Multichannel capailities. It's possible to specify the number of simultaneous TX/RX allowed in the nodes (i.e., number of radios)
* mobility
	* RWM: Random Walk Model. Nodes move randomly
	* RPGM: Reference Point Group Mobility with obstacles. Nodes move in gorup and avoid obstacles by a Virtual Force Field
	* Structured mesh. A mesh network can be built with a specific hop average
* scheduler
	* DeBraS: Aloha and TDMA DeBraS. Different number of DeBraS cells can be specified. A more realistic implementation is included by adding 		  max payload and "Fresheness" to DeBraS
	* P-centralized: A centralized scheduler that has total knowledge of the network has been included.
* traffic model
	* Pareto variable traffic for Hurst H=0.6 and average pkPeriod

Running
-------

* Run a simulation:
`python runSimAllCPUs.py $nodes $scheduler $numDeBraS $rpl $sf0 $sixtop $topo $maxnumhops $squareSide $mobility $numRadios $trafficType`

$nodes = 							Number of nodes
$scheduler = opt2 | none | deBras				Where opt2: P-centralized, none: sf0, deBras: DeBraS
$numDeBraS = 							Number of DeBraS cells per channel
$rpl = 								RPL DIO period
$sf0 = 								SF0 HouseKeeping Period
$sixtop = 							6Top HouseKeeping Period
$topo = star | mesh | mesh-struct				Topology: Star topology, random mesh, strcutured mesh
$maxnumhops = 							Max number of hops expected in the network
$squareSide = 			 				For n hops: squareSide = $maxnumhops*0.5
$mobility = static | staticUNI | staticRay | RWM | RPGM 	Mobility models
$numRadios = 							Number of simulatenous TX/RX at every node
$trafficType = constant | paretovariable 			Traffic pattern


The 6TiSCH Simulator
====================

Brought to you by:

* Thomas Watteyne (watteyne@eecs.berkeley.edu)
* Kazushi Muraoka (k-muraoka@eecs.berkeley.edu)
* Nicola Accettura (nicola.accettura@eecs.berkeley.edu)
* Xavier Vilajosana (xvilajosana@eecs.berkeley.edu)

Scope
-----

6TiSCH is an active IETF standardization working group which defines mechanisms to build and maintain communication schedules in tomorrow's Internet of (Important) Things. This simulator allows you to measure the performance of those different mechanisms under different conditions.

What is simulated:

* protocols
    * IEEE802.15.4e-2012 TSCH (http://standards.ieee.org/getieee802/download/802.15.4e-2012.pdf)
    * RPL (http://tools.ietf.org/html/rfc6550)
    * 6top (http://tools.ietf.org/html/draft-wang-6tisch-6top-sublayer)
    * On-The-Fly scheduling (http://tools.ietf.org/html/draft-dujovne-6tisch-on-the-fly)
* the "Pister-hack" propagation model with collisions
* the energy consumption model taken from
    * [A Realistic Energy Consumption Model for TSCH Networks](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=6627960&url=http%3A%2F%2Fieeexplore.ieee.org%2Fiel7%2F7361%2F4427201%2F06627960.pdf%3Farnumber%3D6627960). Xavier Vilajosana, Qin Wang, Fabien Chraim, Thomas Watteyne, Tengfei Chang, Kris Pister. IEEE Sensors, Vol. 14, No. 2, February 2014.

What is *not* simulated:

* downstream traffic

More about 6TiSCH:

| what             | where                                                               |
|------------------|---------------------------------------------------------------------|
| charter          | http://tools.ietf.org/wg/6tisch/charters                            |
| data tracker     | http://tools.ietf.org/wg/6tisch/                                    |
| mailing list     | http://www.ietf.org/mail-archive/web/6tisch/current/maillist.html   |
| source           | https://bitbucket.org/6tisch/                                       |

Gallery
-------

|  |  |  |
|--|--|--|
| ![](https://bytebucket.org/6tisch/simulator/raw/master/examples/run_0_topology.png) | ![](https://bytebucket.org/6tisch/simulator/raw/master/examples/run_0_timelines.png) | ![](https://bytebucket.org/6tisch/simulator/raw/master/examples/gui.png) |

Installation
------------

* Install Python 2.7
* Clone or download this repository
* To plot the graphs, you need Matplotlib and scipy. On Windows, Anaconda (http://continuum.io/downloads) is a good on-stop-shop.

Running
-------

* Run a simulation: `bin/simpleSim/runSim.py`
* Plot fancy graphs: `bin/simpleSim/plotStuff.py`

Use `bin/simpleSim/runSim.py --help` for a list of simulation parameters. In particular, use `--gui` for a graphical interface.

Code Organization
-----------------

* `bin/`: the script for you to run
* `SimEngine/`: the simulator
    * `Mote.py`: Models a 6TiSCH mote running the different standards listed above.
    * `Propagation.py`: Wireless propagation model.
    * `SimEngine.py`: Event-driven simulation engine at the core of this simulator.
    * `SimSettings.py`: Data store for all simulation settings.
    * `SimStats.py`: Periodically collects statistics and writes those to a file.
    * `Topology.py`: creates a topology of the motes in the network.
* `SimGui/`: the graphical user interface to the simulator

Issues and bugs
---------------

* Report at https://bitbucket.org/6tsch/simulator/issues
