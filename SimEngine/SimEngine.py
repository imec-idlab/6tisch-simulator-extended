#!/usr/bin/python
'''
\brief Discrete-event simulation engine.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
Added changes for 6tisch-sim-extended: Esteban Municio <esteban.municio@uantwerpen.be>
'''



#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('SimEngine')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import threading

import Propagation
import Topology
import Mote
import SimSettings
import inspect
import random

#============================ defines =========================================

#============================ body ============================================

class SimEngine(threading.Thread):
    
    #===== start singleton
    _instance      = None
    _init          = False
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SimEngine,cls).__new__(cls, *args, **kwargs)
        return cls._instance
    #===== end singleton
    
    def __init__(self,runNum=None,failIfNotInit=False):
        
        if failIfNotInit and not self._init:
            raise EnvironmentError('SimEngine singleton not initialized.')
        
        #===== start singleton
        if self._init:
            return
        self._init = True
        #===== end singleton
       
        # store params
        self.runNum                         = runNum
        
        # local variables
        self.dataLock                       = threading.RLock()
        self.pauseSem                       = threading.Semaphore(0)
        self.simPaused                      = False
        self.goOn                           = True
        self.asn                            = 0
        self.startCb                        = []
        self.endCb                          = []
        self.events                         = []
        self.settings                       = SimSettings.SimSettings()
	self.propagation                    = Propagation.Propagation()
	self.motes                          = [Mote.Mote(id) for id in range(self.settings.numMotes)]

	#before create topology, define the obstacles
	if self.settings.mobilityModel=='RPGM':	
		#set a destination for all motes in RPGM
		self.setNewDestination()
		self.margin=0.02
		#self.obstacles="4squares"
		self.obstacles="2rectangles"
	else:
		self.obstacles="none"
       
	#check maxNumHops
	if self.settings.maxNumHops!='x':
		self.settings.maxNumHops=int(self.settings.maxNumHops)
        self.topology                       = Topology.Topology(self.motes)
        self.topology.createTopology()
       
	#dictionaries for init the experiment
	self.joiningTime={}
	self.nodeHasTxCellsTime={}
	self.nodeSendingTime={}
	   
        # boot all motes
        for i in range(len(self.motes)):
            self.motes[i].boot()
        
        self.initTimeStampTraffic          = 0
        self.endTimeStampTraffic           = 0       
        
        # initialize parent class
        threading.Thread.__init__(self)
        self.name                           = 'SimEngine'

     	
	#total TX and RX of data packets
        self.totalTx=0
        self.totalRx=0

	#all messages sent and received: data, 6top and RPL
	self.TRX=0   	
        self.RDX=0

	#duration of the experiment
        self.timeElapsedFlow=0        

	#drops in the PHY layer
        self.dropByCollision=0
        self.dropByPropagation=0

	#debras stats
        self.deBrasReceived=0
        self.deBrasTransmitted=0


	self.packetsSentToRoot=0  #total packets sent from all nodes to the root node
        self.packetReceivedInRoot=0 #total packets sent from all nodes to the root node
	self.olGeneratedToRoot=0  #average throughput generated	
	self.thReceivedInRoot=0   #average throughput received
	self.pkprobeGeneratedToRoot=0  #packet probe generated	
	self.pkprobeReceivedInRoot=0   #packet probe received

	#mote object that is the dagroot
	self.dagRoot=None

	#the saturation has been forced to start due to saturation reached or time limit exceeded
	self.simulationForced=False
	           
	#initial values that will be overwritten in SimStats
        self.experimentInitTime=0
        self.experimentEndTime=self.settings.numCyclesPerRun

	#specifies the turn for a tidy joining process	
	self.turn=1

	#flag for saturation
	self.saturationReached=False


    def checkConvergence(self):
	'''
	Used for knowing if the experiment can start
	'''
	if self.saturationReached==True: #simulation has been forced by saturation reached
	    return True

	if self.simulationForced==True:	#simulation has been forced by either time exceeded or saturation reached
	    return True

	totThReq=0
	totThTxCells=0
	for m1 in self.motes:
	    totThReq=totThReq+m1.threq
	for m2 in self.motes:	
	    totThTxCells=totThTxCells+len(m2.getTxCells())

	if totThTxCells > totThReq:
		#enough cells have been allocated, experiment can start
		return True
	else:
		#not enough cells have been allocated. Do not start experiment yet
		return False
  

    def setNewDestination(self):
	''' 
	Defines the destination for all motes
	'''
	self.destx=2.20
	self.desty=0.203

	for mote in self.motes:
		mote.destx=2.20
		mote.desty=0.203


    def checkValidPosition(self, xcoord,ycoord,countSquare):
	''' 
	Checks if a given postition is valid when moving
	'''
	if self.obstacles=='2rectangles':
		return self.checkValidPosition2rectangles(xcoord,ycoord,countSquare)
	if self.obstacles=='4squares':
		return self.checkValidPosition4squares(xcoord,ycoord,countSquare)
	if self.obstacles=='none':
		return True
	else:
		assert False

    def checkValidPositionMotePlacement(self, xcoord,ycoord,countSquare):
	''' 
	Checks if a given postition is valid when initially placing
	'''
	if self.obstacles=='2rectangles':
		return self.checkValidPosition2rectanglesMotePlacement(xcoord,ycoord,countSquare)
	if self.obstacles=='4squares':
		assert False #not implemented
	if self.obstacles=='none':
		return True
	else:
		assert False


    def checkValidPosition2rectangles(self, xcoord,ycoord,countSquare):	
	''' 
	Checks if a given postition is inside the 2 rectangles (obstacles) with a smaller margin when motes are moving
	'''
	inSquare=False	#total area
	insideObstacle1=False	#rectangle 1
	insideObstacle2=False	#rectangle 2
	if countSquare:
		if (xcoord<self.settings.squareSide and ycoord<self.settings.squareSide) and (xcoord > 0 and ycoord > 0):
			inSquare=True		
	else:
		inSquare=True
	
	if (xcoord<(1.6+self.margin)) and (ycoord>(0.5-self.margin) and (ycoord<(1+self.margin))):
		insideObstacle1=True
	if (xcoord>(1-self.margin)) and (ycoord>(1.5-self.margin)):
		insideObstacle2=True

	if inSquare and not insideObstacle1 and not insideObstacle2:			
		return True
	else:
		return False


    def checkValidPosition2rectanglesMotePlacement(self, xcoord,ycoord,countSquare):
	''' 
	Checks if a given postition is inside the 2 rectangles (obstacles) with a bigger margin (margin + 10 meters) when motes are initially placed
	'''	
	inSquare=False	#total area
	insideObstacle1=False	#rectangle 1
	insideObstacle2=False	#rectangle 2
	notoutsidethebottomleft=False	#for placing, only locate in the left down corner
	if countSquare:
		if (xcoord<self.settings.squareSide and ycoord<self.settings.squareSide) and (xcoord > 0 and ycoord > 0):
			inSquare=True		
	else:
		inSquare=True
	
	if (xcoord<(1.6+self.margin+0.01)) and (ycoord>(0.5-self.margin-0.01) and (ycoord<(1+self.margin+0.01))):
		insideObstacle1=True
	if (xcoord>(1-self.margin-0.01)) and (ycoord>(1.5-self.margin-0.01) ):
		insideObstacle2=True
	if (xcoord< 1.25) and (ycoord>1.25):
		notoutsidethebottomleft=True

	if inSquare and not insideObstacle1 and not insideObstacle2 and notoutsidethebottomleft:			
		return True
	else:
		return False
	
  
    def destroy(self):	
        # destroy the propagation singleton
        self.propagation.destroy()
        
        # destroy my own instance
        self._instance                      = None
        self._init                          = False
  	
    #======================== thread ==========================================
    
    def run(self):
        ''' event driven simulator, this thread manages the events '''

        # log
        log.info("thread {0} starting".format(self.name))

        # schedule the endOfSimulation event
        self.scheduleAtAsn(
            asn         = self.settings.slotframeLength*self.settings.numCyclesPerRun,	
            cb          = self._actionEndSim,
            uniqueTag   = (None,'_actionEndSim'),
        )
        
        # call the start callbacks
        for cb in self.startCb:
            cb()
        
        # consume events until self.goOn is False
        while self.goOn:
            
            with self.dataLock:
                
                # abort simulation when no more events
                if not self.events:
                    log.info("end of simulation at ASN={0}".format(self.asn))
                    break
                
                #emunicio, to avoid errors when exectuing step by step with large networks
		(a,b,cb,c)=self.events[0]
                if c[1]!='_actionPauseSim':   
			# make sure we are in the future              
                       assert self.events[0][0] >= self.asn

                # update the current ASN
                self.asn = self.events[0][0]
                
                # call callbacks at this ASN
                while True:                 
                    if self.events[0][0]!=self.asn:
                        break
                    (_,_,cb,_) = self.events.pop(0)
                    cb()
        
        # call the end callbacks
        for cb in self.endCb:
            cb()
        
        # log
        log.info("thread {0} ends".format(self.name))
    
    #======================== public ==========================================

    def getAvgVisibleNeighbors(self):
	''' 
	Return the average number of visible neighbors
	'''
	total=0.0
	for mote in self.motes:
		total+=len(mote.numVisibleNeighbors)

	return total/len(self.motes)

    def setNodeHasTxCellsTime(self,mote):
	''' 
	set the time for when a mote get his first TX cell
	'''	
	self.nodeHasTxCellsTime[mote]=self.asn

    def getMaxNodeHasTxCellsTime(self):
	''' 
	Return the time when all motes got a TX cell
	'''
	return self.nodeHasTxCellsTime[max(self.nodeHasTxCellsTime, key=self.nodeHasTxCellsTime.get)]

    def setNodeSendingTime(self,mote):	
	''' 
	set the time for when a mote start to send
	'''
	self.nodeSendingTime[mote]=self.asn

    def getMaxNodeSendingTime(self):
	''' 
	Return the time when all motes have started to send
	'''
	return self.nodeSendingTime[max(self.nodeSendingTime, key=self.nodeSendingTime.get)]

    def setJoiningTime(self,mote):
	''' 
	set the time for when a mote has got a parent
	'''	
	self.joiningTime[mote]=self.asn

    def getMaxJoiningTime(self):
	''' 
	Return the time when all motes got a parent
	'''
	return self.joiningTime[max(self.joiningTime, key=self.joiningTime.get)]

    def incrementStatDropByCollision(self):
        self.dropByCollision+=1
     
    def incrementStatDropByPropagation(self):  
        self.dropByPropagation+=1

    def incrementStatTRX(self):  
        self.TRX+=1

    def incrementStatRDX(self):  
        self.RDX+=1

    #=== scheduling
    
    def scheduleEndSimAt(self,initcycle,endcycle):
	''' used set dynamically the end of the simulation '''

	self.experimentInitTime=initcycle
	self.experimentEndTime=endcycle

	self.scheduleAtAsn(
            asn         = self.settings.slotframeLength*self.experimentEndTime,	
            cb          = self._actionEndSim,
            uniqueTag   = (None,'_actionEndSim'),
        )

    def scheduleAtStart(self,cb):
        with self.dataLock:
            self.startCb    += [cb]
    
    def scheduleIn(self,delay,cb,uniqueTag=None,priority=0,exceptCurrentASN=True):
        ''' used to generate events. Puts an event to the queue '''
        
        with self.dataLock:
            asn = int(self.asn+(float(delay)/float(self.settings.slotDuration)))

            self.scheduleAtAsn(asn,cb,uniqueTag,priority,exceptCurrentASN)
    
    def scheduleAtAsn(self,asn,cb,uniqueTag=None,priority=0,exceptCurrentASN=True):
        ''' schedule an event at specific ASN '''
        
        # make sure we are scheduling in the future
        assert asn>self.asn
        
        # remove all events with same uniqueTag (the event will be rescheduled)
        if uniqueTag:
            self.removeEvent(uniqueTag,exceptCurrentASN)
        
        with self.dataLock:
            
            # find correct index in schedule
            i = 0
            while i<len(self.events) and (self.events[i][0]<asn or (self.events[i][0]==asn and self.events[i][1]<=priority)):
                i +=1
            
            # add to schedule
            self.events.insert(i,(asn,priority,cb,uniqueTag))            
    
    def removeEvent(self,uniqueTag,exceptCurrentASN=True):
        with self.dataLock:
            i = 0
            while i<len(self.events):
                if self.events[i][3]==uniqueTag and not (exceptCurrentASN and self.events[i][0]==self.asn):
                    self.events.pop(i)
                else:
                    i += 1
    
    def scheduleAtEnd(self,cb):
        with self.dataLock:
            self.endCb      += [cb]
    
    #=== play/pause
    
    def play(self):
        self._actionResumeSim()
    
    def pauseAtAsn(self,asn):
        if not self.simPaused:
            self.scheduleAtAsn(
                asn         = asn,
                cb          = self._actionPauseSim,
                uniqueTag   = ('SimEngine','_actionPauseSim'),
            )
    
    #=== getters/setters
    
    def getAsn(self):
        return self.asn
       
    #======================== private =========================================
    
    def _actionPauseSim(self):
        if not self.simPaused:
            self.simPaused = True
            self.pauseSem.acquire()
    
    def _actionResumeSim(self):
        if self.simPaused:
            self.simPaused = False
            self.pauseSem.release()
    
    def _actionEndSim(self):
        
        with self.dataLock:
            self.goOn = False            	    
            for mote in self.motes:
                mote._log_printEndResults()
