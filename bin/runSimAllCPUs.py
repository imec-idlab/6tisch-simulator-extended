#!/usr/bin/python
'''
\brief Start batch of simulations concurrently.
Workload is distributed equally among CPU cores.
\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
Added changes for 6tisch-sim-extended: Esteban Municio <esteban.municio@uantwerpen.be>
'''

import os
import time
import math
import multiprocessing

MIN_TOTAL_RUNRUNS = 1

def runOneSim(params):

    (cpuID,numRuns,numMotes,scheduler,numBroad,rpl,otf,top,maxnumhops,squareside,mobility,numradios,trafficType) = params
    command     = []
    command    += ['python ./runSimOneCPU.py']  
    command    += ['--numRuns {0}'.format(numRuns)]
    command    += ['--numMotes {0}'.format(numMotes)] 
    command    += ['--scheduler {0}'.format(scheduler)]
    command    += ['--topology {0}'.format(topology)]
    command    += ['--simDataDir simData_{3}-{6}-hops{5}-deBrasCells{4}-numRadios{7}_rpl_{0}_otf_{1}_sixtop_{2}_trafficType_{8}'.format(rpl,otf,top,scheduler,numBroad,maxnumhops,mobility,numradios,trafficType)]
    command    += ['--maxNumHops {0}'.format(maxnumhops)]
    command    += ['--mobilityModel {0}'.format(mobility)]      
    command    += ['--numDeBraSCells {0}'.format(numBroad)]
    command    += ['--dioPeriod {0}'.format(rpl)]
    command    += ['--squareSide {0}'.format(squareside)]
    command    += ['--otfHousekeepingPeriod {0}'.format(otf)]
    command    += ['--sixtopHousekeepingPeriod {0}'.format(top)]
    command    += ['--cpuID {0}'.format(cpuID)]
    command    += ['--numRadios {0}'.format(numradios)]
    command    += ['--trafficType {0}'.format(trafficType)]
    command     = ' '.join(command)

    os.system(command)

def printProgress(num_cpus):
    while True:
        time.sleep(2)
        output     = []
        for cpu in range(num_cpus):
            with open('cpu{0}.templog'.format(cpu),'r') as f:
                output += ['[cpu {0}] {1}'.format(cpu,f.read())]
        allDone = True
        for line in output:
            if line.count('ended')==0:
                allDone = False
        output = '\n'.join(output)

        if allDone:
            break
    for cpu in range(num_cpus):
        os.remove('cpu{0}.templog'.format(cpu))

if __name__ == '__main__':

    #reading parameters
    numMotes=os.sys.argv[1]
    scheduler=os.sys.argv[2]
    numBroad=os.sys.argv[3]
    rpl=os.sys.argv[4]
    otf=os.sys.argv[5]
    top=os.sys.argv[6]
    topology=os.sys.argv[7]
    maxnumhops=os.sys.argv[8]
    squareside=os.sys.argv[9]
    mobility=os.sys.argv[10]
    numradios=os.sys.argv[11]
    trafficType=os.sys.argv[12]

    multiprocessing.freeze_support()
    #num_cpus = multiprocessing.cpu_count()
    num_cpus = MIN_TOTAL_RUNRUNS	#specify manually the number of CPUs -> 1 CPU per run
    runsPerCpu = int(math.ceil(float(MIN_TOTAL_RUNRUNS)/float(num_cpus)))
    pool = multiprocessing.Pool(num_cpus)
    pool.map_async(runOneSim,[(i,runsPerCpu,numMotes,scheduler,numBroad,rpl,otf,top,maxnumhops,squareside,mobility,numradios,trafficType) for i in range(num_cpus)])
    printProgress(num_cpus)
    print "Finish."
