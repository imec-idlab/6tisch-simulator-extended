#!/usr/bin/python
'''
\brief Model of a 6TiSCH mote.

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
log = logging.getLogger('Mote')
log.setLevel(logging.DEBUG)
log.addHandler(NullHandler())

#============================ imports =========================================

import copy
import random
import threading
import math

import SimEngine
import SimSettings
import Propagation
import Topology

#============================ defines =========================================

#============================ body ============================================

class Mote(object):
    
    # sufficient num. of tx to estimate pdr by ACK
    NUM_SUFFICIENT_TX                  = 10
    # maximum number of tx for history
    NUM_MAX_HISTORY                    = 32
    
    DIR_TX                             = 'TX'
    DIR_RX                             = 'RX'
    DIR_SHARED                         = 'SHARED'
       
    DEBUG                              = 'DEBUG'
    INFO                               = 'INFO'
    WARNING                            = 'WARNING'
    ERROR                              = 'ERROR'
    
    #=== app
    APP_TYPE_DATA                      = 'DATA'
    RPL_TRAFFIC		               = 'RPLTRAFFIC'
    SIXP_TYPE_MYSCHEDULE               = 'DEBRAS'
    SIXTOP_CMD_TRAFFIC		       = 'SIXTOP_CMD'   
    #=== rpl
    RPL_PARENT_SWITCH_THRESHOLD        = 384 #768  corresponds to 1.5 hops. 6tisch minimal draft use 384 for 2*ETX.
    #RPL_MIN_HOP_RANK_INCREASE          = 256
    RPL_MIN_HOP_RANK_INCREASE          = 1536
    RPL_MAX_ETX                        = 1.3
    RPL_MAX_RANK_INCREASE              = RPL_MAX_ETX*RPL_MIN_HOP_RANK_INCREASE*2 # 4 transmissions allowed for rank increase for parents
    RPL_MAX_TOTAL_RANK                 = 256*RPL_MIN_HOP_RANK_INCREASE*2 # 256 transmissions allowed for total path cost for parents
    RPL_PARENT_SET_SIZE                = 1
    DEFAULT_DIO_INTERVAL_MIN           = 3 # log2(DIO_INTERVAL_MIN), with DIO_INTERVAL_MIN expressed in ms
    DEFAULT_DIO_INTERVAL_DOUBLINGS     = 20 # maximum number of doublings of DIO_INTERVAL_MIN (DIO_INTERVAL_MAX = 2^(DEFAULT_DIO_INTERVAL_MIN+DEFAULT_DIO_INTERVAL_DOUBLINGS) ms)
    DEFAULT_DIO_REDUNDANCY_CONSTANT    = 10 # number of hearings to suppress next transmission in the current interval
    
    #=== 6top states
    IDLE                              = 0x00
    # sending
    SIX_STATE_SENDING_REQUEST                   = 0x01
    # waiting for SendDone confirmation
    SIX_STATE_WAIT_ADDREQUEST_SENDDONE          = 0x02  
    SIX_STATE_WAIT_DELETEREQUEST_SENDDONE       = 0x03
    # waiting for response from the neighbor
    SIX_STATE_WAIT_ADDRESPONSE                  = 0x07
    SIX_STATE_WAIT_DELETERESPONSE               = 0x08 
    #response senddone
    SIX_STATE_REQUEST_RECEIVED                  = 0x0c
    SIX_STATE_WAIT_RESPONSE_SENDDONE            = 0x0d

    #=== 6top commands
    IANA_6TOP_CMD_ADD			        = 0x01 
    IANA_6TOP_CMD_DELETE   			= 0x02
    IANA_6TOP_RC_SUCCESS   			= 0x06
    IANA_6TOP_RC_ERR       			= 0x0c
    #TODO: Implement other commands: COUNT, LIST, RELOCATE

    #otf
    OTF_TRAFFIC_SMOOTHING              = 0.5
    #=== 6top
    #=== tsch
    TSCH_QUEUE_SIZE                    = 10
    TSCH_MAXTXRETRIES                  = 5    
    #=== radio
    RADIO_MAXDRIFT                     = 30 # in ppm
    #=== battery
    # see A Realistic Energy Consumption Model for TSCH Networks.
    # Xavier Vilajosana, Qin Wang, Fabien Chraim, Thomas Watteyne, Tengfei
    # Chang, Kris Pister. IEEE Sensors, Vol. 14, No. 2, February 2014.
    CHARGE_Idle_uC                     = 24.60
    CHARGE_TxDataRxAck_uC              = 64.82
    CHARGE_TxData_uC                   = 49.37
    CHARGE_RxDataTxAck_uC              = 76.90
    CHARGE_RxData_uC                   = 64.65
    
    def __init__(self,id):
        
     
    
        # store params
        self.id                        = id
        # local variables
        self.dataLock                  = threading.RLock()
        
        self.engine                    = SimEngine.SimEngine()
        self.settings                  = SimSettings.SimSettings()
        self.propagation               = Propagation.Propagation()
	
        # app
        self.pkPeriod                  = self.settings.pkPeriod     

	#bursty traffic 
	self.pendingBursts=0			#num burst sent during the simulation
	#shape for pareto variable traffic
	self.a= 1.8  # shape Hurst=0.6
	#scale for pareto variable traffic
	self.m=(self.settings.pkPeriod*(self.a-1)/(self.a)) #scale is 0.448888889 for H=0.6 and pkPeriod=1.01

   
        # role
        self.dagRoot                   = False
        # rpl
        self.rank                      = None
        self.dagRank                   = None
        self.parentSet                 = []
        self.preferredParent           = None
        self.rplRxDIO                  = {}                    # indexed by neighbor, contains int
        self.neighborRank              = {}                    # indexed by neighbor
        self.neighborDagRank           = {}                    # indexed by neighbor
        self.trafficPortionPerParent   = {}                    # indexed by parent, portion of outgoing traffic

	self.DIOpackets=True				#enable realistic RPL
	self.moteConnectedDioPeriodIncreaseFactor=1	#implements a simple trickle algorithm
	self.previousParent=None			#tracks the previous parent of a mote
	self.rplParentChangeOperationInCourse=False	#tracks if there is a parent change operation


        # otf
        self.asnOTFevent               = None
        self.otfHousekeepingPeriod     = self.settings.otfHousekeepingPeriod
        self.timeBetweenOTFevents      = []
        self.inTraffic                 = {}                    # indexed by neighbor
        self.inTrafficMovingAve        = {}                    # indexed by neighbor
        # 6top
        self.numCellsToNeighbors       = {}                    # indexed by neighbor, contains int
        self.numCellsFromNeighbors     = {}                    # indexed by neighbor, contains int

	self.sixtopState=self.IDLE		#sixtop state
	self.responseType=None			#sixtop type response
	self.cellsPendingOperation=None		#The node who trigger the CMD remember the operation that he was requesting
	self.cellsPendingOperationType=None	#The node who trigger the CMD remember the direction of the cells
	self.cellsPendingOperationNeigh=None	#The node who trigger the CMD remember the cells that he was requesting
	self.timeoutAdd=0			#timeouts for when a add response is not expected anymore
	self.timeoutDel=0			#timeouts for when a delete response is not expected anymore
        
        # changing this threshold the detection of a bad cell can be
        # tuned, if as higher the slower to detect a wrong cell but the more prone
        # to avoid churn as lower the faster but with some chances to introduces
        # churn due to unstable medium
        self.sixtopPdrThreshold           = self.settings.sixtopPdrThreshold
        self.sixtopHousekeepingPeriod  = self.settings.sixtopHousekeepingPeriod
        # tsch
        self.txQueue                   = []
	self.txSharedQueue             = []
        self.pktToSend                 = []             #list of packets to send in one ts (in different channels)	
	self.pendingAck		       = [] 		#record the expected ack in a timeslot (for the different channels when num radios > 1)
        self.schedule                  = {}             # indexed by ts and ch  contains info of the all the channels in each ts 
        self.scheduleNeigborhood       = {}             # indexed by neighbour contains the cells used in my neighborhood  
	self.numVisibleNeighbors=0			#tracks the number of PHY neighbors of a more

	self.numberOfWaitings=None		#number of remaining attempts before transmit a packet in a SHARED cell
	self.maxWinShared=0			#max window size of the SHARED cells                 
        
        #self.waitingFor                = None               #not used, usinf sdr
        self.timeCorrectedSlot         = None
        # radio
        self.txPower                   = 0                     # dBm
        self.antennaGain               = 0                     # dBi
        self.minRssi                   = self.settings.minRssi # dBm
        self.noisepower                = -105                  # dBm
        self.drift                     = random.uniform(-self.RADIO_MAXDRIFT, self.RADIO_MAXDRIFT)
        # wireless
        self.RSSI                      = {}                    # indexed by neighbor
        self.PDR                       = {}                    # indexed by neighbor

        # location

	#coordinates of the mote
        self.x=0
        self.y=0 
 
	#mobility
	#destination for RPGM
	self.destx=0
	self.desty=0

        # battery
        self.chargeConsumed            = 0

	#phy        
	self.staticPhys       = {}	#save the initial value form this node with it's neighbours
	#PDR taken in account to average numACK when there is still no acks
	self.firstPDR={}

	#DEBRAS
	self.DEBRASALOHA=True	# use ALOHA or TDMA
        self.maxWin=None	#max window for DEBRAS ALOHA
	#cell assigned for DEBRAS TDMA
	self.myBrTs=None	#ts
        self.myBrCh=None	#ch
	self.MAXCELLSDEBRASPAYLOAD=36	#limitation in the packet size (3byte x 36) + 17 < 127
	self.deBrasTransmitted=0	#num of debras packet transmitted
        self.deBrasReceived=0		#num of debras packet received
	self.numberOfWaitingsDeBras=None	#number of remaining attempts before transmit a DEBRAS packet

        # stats
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()
        
        #emunicio stats        
        self.packetsGenerated=0		#total data packets generated during the whole simulation at APP level
	self.numPacketSent= 0 		#total data packets generated during the whole simulation that are actually enqueued 
	self.numPacketReceived= 0 	#number of packets received 
        self.probePacketsGenerated=0	#total data packets generated during the experiment
	self.probeNumPacketReceived=0   #total data packets generated during the experiment
        self.numTransmissions=0		#numTX
        self.numReceptions=0		#numRX

        self.numReqCells=0		#track the required cells by SF0
        self.hopsToRoot=0		#number of hops to root		
	self.threq=0			#track the theoretical cell demand (in terms of number of hops to the root)
	#tracks the the number of randomly selected cells in 6top. Always in SF0, only in saturation with debras and opt2
        self.numRandomSelections=0	

	#init
	self.sendingPacketsTriggered=False	#the mote has started to send packets
	self.otfTriggered=False  		#the mote has triggered otf
	self.moteJoined=0			#the node has joined the network
	self.moteWithTxCells=0			#the node has tx cells		
	self.moteSending=0			#the node is sending data packets

    #======================== stack ===========================================
    
    #===== role
    
    def role_setDagRoot(self):
        self.dagRoot              = True
        self.rank                 = 0
        self.dagRank              = 0
        self.packetLatencies      = [] # in slots
        self.packetHops           = []
        
    
    #===== application
    
    def _app_schedule_sendSinglePacket(self,firstPacket=False):
        '''
        create an event that is inserted into the simulator engine to send the data according to the traffic
        '''  
        with self.dataLock:
                if not firstPacket:
		    if self.settings.trafficType=="constant":
                    	# compute random delay
                    	delay            = self.pkPeriod*(1+random.uniform(-self.settings.pkPeriodVar,self.settings.pkPeriodVar))

		    if self.settings.trafficType=="paretovariable":
			delay = random.paretovariate(self.a) * self.m
                else:
                    # compute initial time in terms of the id for a secuential start
		    #(to speed up the process, take in account the num shared cells and the num radios)		    
                    delay = 1 + 10*random.random()+(2*self.id/(self.settings.numSHAREDCells*self.settings.numRadios))		    
                assert delay>0   

                self.engine.scheduleIn(
                        delay            = delay,
                        cb               = self._app_action_sendSinglePacket,
                        uniqueTag        = (self.id, '_app_action_sendSinglePacket'),
                        priority         = 2,
                )
                  
    def _app_schedule_sendPacketBurst(self):
        ''' create an event that is inserted into the simulator engine to send a data burst'''
	
	#burstTimestamp is neglected. Instead used a random value for each mote
	asndelay=int(random.uniform(1,199))*101+self.engine.asn	#assuming 200 cycles of experiment
	
        # schedule numPacketsBurst packets at burstTimestamp
        for i in xrange(self.settings.numPacketsBurst):
	    self.engine.scheduleAtAsn(
                asn         = asndelay,
                cb          = self._app_action_enqueueData,
                uniqueTag   = (self.id,'_app_action_enqueueData_burst{0}'.format(asndelay*random.random())),
                priority    = 2,
            )

	self.pendingBursts=self.pendingBursts-1
    
    def _app_action_sendSinglePacket(self):
        ''' actual send data function. Evaluates queue length too '''

	#root no sending	
	if self.id!=0:
		# schedule next _app_action_sendSinglePacketNew destination
		self._app_action_enqueueData()
		self._app_schedule_sendSinglePacket()

		#if simulation started and bursty traffic is enabled, schedule 1 burst
		if self.pendingBursts==1:
		    if self.engine.experimentEndTime!=10000:	#experiment has started	    
		        self._app_schedule_sendPacketBurst()
	
    #===== mobility

    def _updateLocation(self):
	''' Update location of the mote'''

	if self.settings.mobilityModel=='RWM': #Random Walk Model (Brownian motion)
		s=16
		speed=float(random.uniform(s*0.8,s*1.2))
		div=1000/speed

		prevx=self.x
		prevy=self.y
		correctlyMoved=False
		while not correctlyMoved:

			rads=2*3.14159*random.random()
			xdelta=math.cos(rads)/div
			ydelta=math.sin(rads)/div
			if self.x+xdelta<self.settings.squareSide and self.y+ydelta<self.settings.squareSide:
				if self.x+xdelta > 0 and self.y+ydelta > 0:
					correctlyMoved=True	
		
		self.setLocation(self.x+xdelta,self.y+ydelta)
	
		distance=math.sqrt(
    			(prevx - self.x)**2 +
    			(prevy - self.y)**2
		)
		
	if self.settings.mobilityModel=='RWP':  #Random Way Point
		assert False	#TODO not implemented

	if self.settings.mobilityModel=='RPGM':  #Reference Point Group Mobility
		(repMod,repAlpha) = self._calculateRepulsionVector()

		#how string is the attraction factor
		repMod=repMod*3.5
		
		s=15.0 #group speed 

		#RP component
		prevx=self.x	#remember previous location
		prevy=self.y	#remember previous location

		#calculated speed for the mote in this RP movement
		speed1=float(random.uniform(s*0.9,s*1.1))*5
						
		#next move
		div=1000/speed1
		destx=self.destx
		desty=self.desty

		#calculate dest angle
		rads=math.atan2((desty-self.y),(destx-self.x))
							
		xdelta=math.cos(rads)/div	#attraction vector
		ydelta=math.sin(rads)/div	#attraction vector

		repX=math.sin(repAlpha)*repMod	#repulsion vector
		repY=math.cos(repAlpha)*repMod	#repulsion vector

		#resulting angle
		alfatot=math.atan2((xdelta+repX),(ydelta+repY))
		
		sran=float(random.uniform(s*0.8,s*1.2))				
		xdelta_mov=math.sin(alfatot)*(sran/1000)	#x, vector result with speed constant: 10 mps 
		ydelta_mov=math.cos(alfatot)*(sran/1000)	#y, vector result with speed constant: 10 mps
								
		#while not correctlyMoved:	#inside the simulation square or outside the objects
		if self.engine.checkValidPosition(self.x+xdelta_mov,self.y+ydelta_mov,True):
			self.setLocation(self.x+xdelta_mov,self.y+ydelta_mov)							
		
			#RM component
			rads=2*3.14159*random.random()	#random angle

			#constant speed of s
			xdelta=math.sin(rads)*(s/1000)	
			ydelta=math.cos(rads)*(s/1000)  	

			#if valid location set the new location
			if self.engine.checkValidPosition(self.x+xdelta,self.y+ydelta,True):
				self.setLocation(self.x+xdelta,self.y+ydelta)
				distance=math.sqrt(
					(prevx - self.x)**2 +
					(prevy - self.y)**2
				)
			#update the distance to the destination
			distance=math.sqrt(
				(destx - self.x)**2 +
				(desty - self.y)**2
			)
			if (1000*distance) <= s*2:  # if remaining meters are below the distance traveled in 1 sec(actually the speed)
				#destination reached. Set a new destination
				self.destx=0.3
				self.desty=0.1


		##If mote is blocked
		else:
			
			#moving towards my parent.
			if self.id!=0:	#root mote cant be blocked
				rads=math.atan2((self.preferredParent.y-self.y),(self.preferredParent.x-self.x))
				xdelta_mov=math.sin(rads)*(s/1000)	#vector result with speed constant: 10 mps
				ydelta_mov=math.cos(rads)*(s/1000)	#vector result
				if self.engine.checkValidPosition(self.x+xdelta_mov,self.y+ydelta_mov,True):
					#usually should not be blocked when going toward the parent
					self.setLocation(self.x+xdelta_mov,self.y+ydelta_mov)					
	
  
    def _calculateRepulsionVector(self,towardsCentroid=False):
	''' Calculate the resulting repulsion vector'''
	repulsionVectorPolar=[]

	#how string is the respulsion factor
	div=8	

	#angle for the attraction
	rads=math.atan2((self.desty-self.y),(self.destx-self.x))
	angle=rads-(3.14159/2)+(0.017453278*60)	#degrees

	for i in xrange(60,120,5):	#the repulsion is checked with an array of size 12  that represent 60 degrees of search
		#from 60 degrees to 120, center at 90
		angle=angle+0.08725 # +5 degree in rads
		xdelta=math.cos(angle)/div
		ydelta=math.sin(angle)/div	
		objectx=self.x
		objecty=self.y
		distance=0.0	
		
		#search obstacles at every angle
		obstacleFound=False
		while not obstacleFound:
			#update distance
			distance=math.sqrt(
	    			(objectx - self.x)**2 +
	    			(objecty - self.y)**2
       			)
			#if not obstacle, check a bit further in the same angle
			if self.engine.checkValidPosition(objectx,objecty,False) and (distance) < 0.4:
				objectx=objectx+xdelta
				objecty=objecty+ydelta
				
			else:
				obstacleFound=True 	#there is an obstacle at this angle and distance
				if distance>=(0.4):					
					distance=0.4	#max is 400m 
				else:
					#obstacles found before 400m, insert point
					repulsionVectorPolar.append([angle,distance])

	#all repulsion component have been calculated		
	self.repulsionVectorCart=[]

	i=0
	for (alfa,mod) in repulsionVectorPolar:
		if (mod)<0.4:	#I only save the vectors that have any obstacle
			if mod<0.2:	#nearest objects have a surplus of repulsion			
			    self.repulsionVectorCart.append([(1.5*math.sin(alfa)*(0.4-mod)),(-1.0*math.cos(alfa)*(0.4-mod))])				
			else:
			    self.repulsionVectorCart.append([(1.0*math.sin(alfa)*(0.4-mod)),(-1.0*math.cos(alfa)*(0.4-mod))])
		i+=1

	sumRepulsionX=0
	sumRepulsionY=0

	#convert to cartesian to sum the components
	for (xval,yval) in self.repulsionVectorCart: 
		sumRepulsionX=sumRepulsionX-xval
		sumRepulsionY=sumRepulsionY+yval
	sumRepulsionX=sumRepulsionX/(len(self.repulsionVectorCart)+1) #(len(self.repulsionVectorCart)+1)
	sumRepulsionY=sumRepulsionY/(len(self.repulsionVectorCart)+1) #(len(self.repulsionVectorCart)+1)
	
	#return the values in polar
	sumRepMod=math.sqrt(sumRepulsionX**2 + sumRepulsionX**2)
	sumRepAlfa=math.atan2(sumRepulsionY,sumRepulsionX)

	return (sumRepMod,sumRepAlfa)

    def _app_action_enqueueData(self):
        ''' enqueue data packet into stack '''
                      
	if self.packetsGenerated==0:	#first time a packet is generated in stationary mode
		self.moteSending+=1
		self.engine.setNodeSendingTime(self) 
	
        self._stats_incrementMoteStats('appGenerated')
        self.packetsGenerated+=1   

	#if the mote is 4 hops to root, he will need 4 cells, 1 per hop
        self.threq=self.hopsToRoot 
    
        if (self.engine.asn > (self.engine.experimentInitTime*self.settings.slotframeLength)): #set probe counter
            self.probePacketsGenerated+=1

	if self.getTxCells():	#cheking if there are tx cells for new packets
	    newPacket = {
		'source':	  self,
		'dest':		  self.preferredParent,
	        'asn':            self.engine.getAsn(),
	        'type':           self.APP_TYPE_DATA,
	        'payload':        [self.id,self.engine.getAsn(),1], # the payload is used for latency and number of hops calculation
	        'retriesLeft':    self.TSCH_MAXTXRETRIES
	    }
	                  
	    # enqueue packet in TSCH queue
	    isEnqueued = self._tsch_enqueue(newPacket,None)
	    
	    if isEnqueued:
		self.numPacketSent+=1
	        self._otf_incrementIncomingTraffic(self)
	    else:	#queue full
	        self._stats_incrementMoteStats('droppedAppFailedEnqueue')
	else:
	    # update mote stats
	    self._stats_incrementMoteStats('droppedAppFailedEnqueue')        
	    self._stats_incrementMoteStats('droppedNoTxCells')
	    

    #=====deBras

    #DEBRAS packets are not enqueued. Are generated every time it's time to transmit in order to send the most updated info

	
    #===== rpl

    def _rpl_schedule_sendDIO(self,firstDIO=False):
        
        with self.dataLock:

            asn    = self.engine.getAsn()
            ts     = asn%self.settings.slotframeLength
            
            if not firstDIO:	
		delay=int(math.ceil(random.uniform(0.5 * self.settings.dioPeriod*self.moteConnectedDioPeriodIncreaseFactor, 1.5 * self.settings.dioPeriod*self.moteConnectedDioPeriodIncreaseFactor) / (self.settings.slotDuration)))
		#once the mote has found a parent and has sent his corresponding DIO increase the DIO period to reduce overhead 
		if self.preferredParent != None or self.id==0:
			if self.moteConnectedDioPeriodIncreaseFactor<10:
		            self.moteConnectedDioPeriodIncreaseFactor=self.moteConnectedDioPeriodIncreaseFactor*2.2
            else:
		if self.id!=0:
			delay=int(math.ceil(random.uniform(0.5 * self.settings.dioPeriod, 1.5 * self.settings.dioPeriod) / (self.settings.slotDuration)))
	        else:
                	delay=100

            if self.preferredParent != None:
                self.hopsToRoot=self.recalculateNumHopsToRoot()
          
            self.engine.scheduleAtAsn(
                asn         = asn+delay,
                cb          = self._rpl_action_sendDIO,
                uniqueTag   = (self.id,'_rpl_action_sendDIO'),
                priority    = 3,
            )
    
    def _rpl_action_sendDIO(self):
        
        with self.dataLock:
            
	    if self.rank!=None :
	            if not self.dagRoot:	#only send DIOs hen you have parent and dagrank
	        	    assert self.preferredParent 
		    	    assert self.dagRank

		    #when forcing star topology, only root can send DIOs to avoid parent changes
		    if (self.settings.topology!='star') or (self.settings.topology=='star' and self.id==0) :

			    #look for RPL packets in the shared queue (a shared queue is used to increase the performance)
			    dioAlreadyInQueue=False
			    for packet in self.txSharedQueue:
			    	if packet['type']=='RPLTRAFFIC':
				    dioAlreadyInQueue=True
			    #if RPL packet already in the queue, do not enqueue			    
			    if dioAlreadyInQueue==False:
				self._rpl_action_enqueueDIO(alreadyinqueue=False)
			    else:
				self._rpl_action_enqueueDIO(alreadyinqueue=True)

	    self._rpl_schedule_sendDIO()


    def _rpl_action_enqueueDIO(self, alreadyinqueue):
	''' enqueue shared data packet into stack to be transmitted in a shared cell '''
	if self.getSharedCells():
	    if not alreadyinqueue:

		    newPacket = {
			'source':	  self,
			'dest':		  None,
			'asn':            self.engine.getAsn(),
			'type':           self.RPL_TRAFFIC,
			'payload':        [self.id,self.engine.getAsn(),self.rank], # the payload is the id, the asn and the rpl rank
			'retriesLeft':    1
		    }
		    
		    # enqueue packet in TSCH queue
		    isEnqueued = self._tsch_enqueue(newPacket,None)

		    if not isEnqueued:
			assert False	#should not happen since no new packet are enqueued when there is already one 

	    #instead of enqueue a new packet, replace the one that already is in the queue
	    else:
		n=0
		for packet in self.txSharedQueue:
		    if packet['type']=='RPLTRAFFIC':
			packet['payload'][2]=self.rank	#update the rank in the packet to send the most recent one
			n+=1	
		assert n<=1	#it is expected to be only one RPL packet at the same time 
	else:
	    print "No shared cells available"
	    assert False

    def _rpl_action_receiveDIO(self,type,smac,payload):

        with self.dataLock:
	    
	    dioSource = smac
	    assert smac.id == payload[0]
            rank = payload[2]

            # update my mote stats
            self._stats_incrementMoteStats('rplRxDIO')

	    # don't update DAGroot
            if self.dagRoot:
                return

	    # don't update poor link
	    if self._rpl_calcRankIncrease(dioSource)>self.RPL_MAX_RANK_INCREASE:
                return

	    # update rank/DAGrank when receiving
            self.neighborDagRank[dioSource]    = int(rank/self.RPL_MIN_HOP_RANK_INCREASE)
            self.neighborRank[dioSource]       = rank

	    self._rpl_housekeeping()

            # in neighbor, update number of DIOs received
            if dioSource not in self.rplRxDIO:
                    self.rplRxDIO[dioSource]=0
            self.rplRxDIO[dioSource]+=1

            # update time correction
            if self.preferredParent == dioSource:
                asn                         = self.engine.getAsn()
                self.timeCorrectedSlot      = asn 

    def _rpl_housekeeping(self):
        with self.dataLock:

            #===
            # refresh the following parameters:
            # - self.preferredParent
            # - self.rank
            # - self.dagRank
            # - self.parentSet
            
            # calculate my potential rank with each of the motes I have heard a DIO from
            potentialRanks = {}
            for (neighbor,neighborRank) in self.neighborRank.items():
                # calculate the rank increase to that neighbor
                rankIncrease = self._rpl_calcRankIncrease(neighbor)
                if rankIncrease!=None and rankIncrease<=min([self.RPL_MAX_RANK_INCREASE, self.RPL_MAX_TOTAL_RANK-neighborRank]): 
                    
                    #check if there is a loop and if exists, skip the neighbor         
		    rootReached=False
		    skipNeighbor=False
	            inode=neighbor
		    while rootReached==False:
			if inode.preferredParent!=None:
			    if inode.preferredParent.id==self.id:
				skipNeighbor=True

			    if inode.preferredParent.id==0:
				rootReached=True
			    else:
				inode=inode.preferredParent
			else:
			    rootReached=True
		    if skipNeighbor==True:
		        continue

		    # record this potential rank
                    potentialRanks[neighbor] = neighborRank+rankIncrease

            # sort potential ranks
            sorted_potentialRanks = sorted(potentialRanks.iteritems(), key=lambda x:x[1])
	    	
            # switch parents only when rank difference is large enough
            for i in range(1,len(sorted_potentialRanks)):
                if sorted_potentialRanks[i][0] in self.parentSet:
                    # compare the selected current parent with motes who have lower potential ranks 
                    # and who are not in the current parent set                  
                    for j in range(i):                    
                        if sorted_potentialRanks[j][0] not in self.parentSet:
                            if sorted_potentialRanks[i][1]-sorted_potentialRanks[j][1]<self.RPL_PARENT_SWITCH_THRESHOLD:
                                mote_rank = sorted_potentialRanks.pop(i)
                                sorted_potentialRanks.insert(j,mote_rank)
                                break

            # pick my preferred parent and resulting rank
            if sorted_potentialRanks:               
                oldParentSet = set([parent.id for parent in self.parentSet])
             
                (newPreferredParent,newrank) = sorted_potentialRanks[0]

                # compare a current preferred parent with new one
                if self.preferredParent and newPreferredParent!=self.preferredParent:
                    for (mote,rank) in sorted_potentialRanks[:self.RPL_PARENT_SET_SIZE]:
                        
                        if mote == self.preferredParent: 			
                            # switch preferred parent only when rank difference is large enough
                            if rank-newrank>self.RPL_PARENT_SWITCH_THRESHOLD:
                                (newPreferredParent,newrank) = (mote,rank)
  
                    # update mote stats
                    self._stats_incrementMoteStats('rplChurnPrefParent')
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: preferredParent {0}->{1}",
                        (self.preferredParent.id,newPreferredParent.id),
                    )
                
                # update mote stats
                if self.rank and newrank!=self.rank:
                    self._stats_incrementMoteStats('rplChurnRank')
                    # log
                    self._log(
                        self.INFO,
                        "[rpl] churn: rank {0}->{1}",
                        (self.rank,newrank),
                    )

		#only change when no other RPL operation is in course and no 6top operation is in course
		if self.rplParentChangeOperationInCourse==False and self.sixtopState==self.IDLE:

		        if self.preferredParent:
			    #mote already had a parent
		            if newPreferredParent!=self.preferredParent:
			        self.previousParent=self.preferredParent	
			        self.rplParentChangeOperationInCourse=True
			    (self.preferredParent,self.rank) = (newPreferredParent,newrank)
			else:

			    #once a parent has been selected in the housekeeping, send a DIO to fast the joining process
			    asn    = self.engine.getAsn()
			    self.engine.scheduleAtAsn(
				asn         = asn+1,
				cb          = self._rpl_action_sendDIO,
				uniqueTag   = (self.id,'_rpl_action_sendDIO'),
				priority    = 3,
			    )			    
			    self.engine.setJoiningTime(self)
			    self.moteJoined+=1
			     # store new preferred parent and rank
			    (self.preferredParent,self.rank) = (newPreferredParent,newrank)
		        
		        # calculate DAGrank
		        self.dagRank = int(self.rank/self.RPL_MIN_HOP_RANK_INCREASE)

		        self.parentSet = [n for (n,_) in sorted_potentialRanks if self.neighborRank[n]<self.rank][:self.RPL_PARENT_SET_SIZE]

		        assert self.preferredParent in self.parentSet
		        
		        if oldParentSet!=set([parent.id for parent in self.parentSet]):
		            self._stats_incrementMoteStats('rplChurnParentSet')

            #===
            # refresh the following parameters:
            # - self.trafficPortionPerParent

            etxs        = dict([(p, 1.0/(self.neighborRank[p]+self._rpl_calcRankIncrease(p))) for p in self.parentSet])
            sumEtxs     = float(sum(etxs.values()))                                    
            self.trafficPortionPerParent = dict([(p, etxs[p]/sumEtxs) for p in self.parentSet])
	    
	    # add TX cells to the new neighbor 
	    if self.rplParentChangeOperationInCourse:
		tsList=[(ts,ch) for (ts,ch), cell in self.schedule.iteritems() if cell['neighbor']==self.previousParent and cell['dir']==self.DIR_TX]
		if len(tsList)==0:
		    self.rplParentChangeOperationInCourse=False
		else:
		    #since the mote has changed parent, has to add new cells to the new parent
		    self._sixtop_cell_reservation_request_action(self.preferredParent,len(tsList))


                       
    def _rpl_calcRankIncrease(self, neighbor):
        
        with self.dataLock:

            etx = self._estimateETX(neighbor)

            if not etx:
                return
            
            # per draft-ietf-6tisch-minimal, rank increase is 2*ETX*RPL_MIN_HOP_RANK_INCREASE
            return int(2*self.RPL_MIN_HOP_RANK_INCREASE*etx)
    
    #===== otf
    
    def _otf_schedule_housekeeping(self,firstOtf=False):
        
	asn    = self.engine.getAsn()
        if firstOtf:
	    delay=0.01	#applying small delay
        else:
            delay=self.otfHousekeepingPeriod*(0.9+0.2*random.random())

        self.engine.scheduleIn(
            delay       = delay,
            cb          = self._otf_action_housekeeping,
            uniqueTag   = (self.id,'_otf_action_housekeeping'),
            priority    = 4,
        )

   
    def _otf_action_housekeeping(self):
        '''
        OTF algorithm: decides when to add/delete cells.
        '''

        with self.dataLock:
            
	    #if there is an RPL operation in course, do not perform SF0 and arrange cell first
	    if self.rplParentChangeOperationInCourse:
			cellsToNewParent=[(ts,ch) for (ts,ch), cell in self.schedule.iteritems() if cell['neighbor']==self.preferredParent and cell['dir']==self.DIR_TX]
			#if have cells with the new parent, remove the cells with the old parent
			if len(cellsToNewParent)>0:
				tsList=[(ts,ch) for (ts,ch), cell in self.schedule.iteritems() if cell['neighbor']==self.previousParent and cell['dir']==self.DIR_TX]
				if len(tsList)>0:
				    self._sixtop_removeCells_request_action(self.previousParent,len(tsList),tsList)
				else:
				    self.rplParentChangeOperationInCourse=False
			#if I dont have cells with the new parent, first get cells to the new parent
			else:
			    if self.sixtopState==self.IDLE:
				tsList=[(ts,ch) for (ts,ch), cell in self.schedule.iteritems() if cell['neighbor']==self.previousParent and cell['dir']==self.DIR_TX]
				self._sixtop_cell_reservation_request_action(self.preferredParent,len(tsList))
		
	    #perform SF0 normally	    
	    else:

		    # calculate the "moving average" incoming traffic, in pkts since last cycle, per neighbor

		    # collect all neighbors I have RX cells to
		    rxNeighbors = [cell['neighbor'] for ((ts,ch),cell) in self.schedule.items() if cell['dir']==self.DIR_RX]

		    # remove duplicates
		    rxNeighbors = list(set(rxNeighbors))
		    
		    # reset inTrafficMovingAve                
		    neighbors = self.inTrafficMovingAve.keys()
		    for neighbor in neighbors: 
		        if neighbor not in rxNeighbors:
		            del self.inTrafficMovingAve[neighbor]

		    # set inTrafficMovingAve 
		    for neighborOrMe in rxNeighbors+[self]:
		        if neighborOrMe in self.inTrafficMovingAve:
		            newTraffic   = 0
		            newTraffic  += self.inTraffic[neighborOrMe]*self.OTF_TRAFFIC_SMOOTHING               # new
		            newTraffic  += self.inTrafficMovingAve[neighborOrMe]*(1-self.OTF_TRAFFIC_SMOOTHING)  # old
		            self.inTrafficMovingAve[neighborOrMe] = newTraffic
		        elif neighborOrMe in self.inTraffic.keys():
				if self.inTraffic[neighborOrMe] != 0:
		            		self.inTrafficMovingAve[neighborOrMe] = self.inTraffic[neighborOrMe]

		    # reset the incoming traffic statistics, so they can build up until next housekeeping
		    self._otf_resetInboundTrafficCounters()
		    
		    # calculate my total generated traffic, in pkt/s
		    genTraffic       = 0
		    # generated/relayed by me
		    for neighborOrMe in self.inTrafficMovingAve:
		        genTraffic  += self.inTrafficMovingAve[neighborOrMe]/self.otfHousekeepingPeriod
		      		    
		    # convert to pkts/cycle
		    genTraffic      *= self.settings.slotframeLength*self.settings.slotDuration

		    #we assume only one parent
		    portion=1.0
		    parent= self.parentSet[0]      
 
		    # calculate required number of cells to that parent
		    etx = self._estimateETX(parent)	
		       
		    if etx>self.RPL_MAX_ETX: # cap ETX
			etx  = self.RPL_MAX_ETX

		    # calculate the OTF threshold
		    threshold     = int(math.ceil(portion*self.settings.otfThreshold))

		    # measure how many cells I have now to that parent
		    nowCells      = self.numCellsToNeighbors.get(parent,0)

		    if etx != None: 
			reqCells      = int(math.ceil(portion*genTraffic*etx))
		    #reqCells is 0 when there is not ETX yet with the parent
		    else:
			reqCells=0

		    self.numReqCells=reqCells

		    if nowCells==0 or nowCells<reqCells:
		            # I don't have enough cells
		            # calculate how many to add
		            if reqCells>0:
		                # according to traffic
		                numCellsToAdd = reqCells-nowCells+(threshold+1)/2
		            else:
		                # but at least one cell		                
		                numCellsToAdd = 1
		            
		            # log
		            self._log(
		                self.INFO,
		                "[otf] not enough cells to {0}: have {1}, need {2}, add {3}",
		                (parent.id,nowCells,reqCells,numCellsToAdd),
		            )
		            
		            # update mote stats
			    if self.sixtopState==self.IDLE:
		            	self._stats_incrementMoteStats('otfAdd')
		            
		            # have 6top add cells
		            self._sixtop_cell_reservation_request_action(parent,numCellsToAdd)
		                                		        
		    elif reqCells<nowCells-threshold:
		            # I have too many cells
		            
		            # calculate how many to remove
		            numCellsToRemove = nowCells-reqCells
		            if reqCells==0:	#I want always there is at least 1 cell available
		                numCellsToRemove=numCellsToRemove-1  

		            # log
		            self._log(
		                self.INFO,
		                "[otf] too many cells to {0}:  have {1}, need {2}, remove {3}",
		                (parent.id,nowCells,reqCells,numCellsToRemove),
		            )
		            
		            # update mote stats
			    if self.sixtopState==self.IDLE:
		            	self._stats_incrementMoteStats('otfRemove')

			    #only remove cells where there is something to remove
			    if numCellsToRemove>0:
		            	self._sixtop_removeCells_request_action(parent,numCellsToRemove,None)
			    
		    #try to start sending packets
	    	    if self.sendingPacketsTriggered==False:	
			#start to send whenever I have at least 1 tx cell	
			if self.getTxCells():
			    if  self.moteWithTxCells==0:
			    	self.moteWithTxCells+=1
				self.engine.setNodeHasTxCellsTime(self)    
			    #start whenever all nodes have at least 1 TX cell
			    if len(self.engine.nodeHasTxCellsTime.keys())==self.settings.numMotes-1:	
		            	self._app_schedule_sendSinglePacket(firstPacket=True)
	    	            	self.sendingPacketsTriggered=True
	    # schedule next housekeeping
            self._otf_schedule_housekeeping()

    def _otf_resetInboundTrafficCounters(self):
        with self.dataLock:
            for neighbor in self._myNeigbors()+[self]:
                self.inTraffic[neighbor] = 0
    
    def _otf_incrementIncomingTraffic(self,neighbor):
        with self.dataLock:
	    if neighbor not in self.inTraffic.keys():
		self.inTraffic[neighbor] = 0		
            self.inTraffic[neighbor] += 1
    	    
    #===== 6top
    
    def _sixtop_schedule_housekeeping(self):
        
        self.engine.scheduleIn(
            delay       = self.sixtopHousekeepingPeriod*(0.9+0.2*random.random()),
            cb          = self._sixtop_action_housekeeping,
            uniqueTag   = (self.id,'_sixtop_action_housekeeping'),
            priority    = 5,
        )
    
    def _sixtop_action_housekeeping(self):
        '''
        For each neighbor I have TX cells to, relocate cells if needed.
        '''
        with self.dataLock:
	        #=== tx-triggered housekeeping 
		
		# collect all neighbors I have TX cells to
		txNeighbors = [cell['neighbor'] for ((ts,ch),cell) in self.schedule.items() if cell['dir']==self.DIR_TX]
		
		# remove duplicates
		txNeighbors = list(set(txNeighbors))

		for neighbor in txNeighbors:
		    nowCells = self.numCellsToNeighbors.get(neighbor,0)

		    assert nowCells == len([t for ((t,ch),c) in self.schedule.items() if c['dir']==self.DIR_TX and c['neighbor']==neighbor])
		
		# do some housekeeping for each neighbor
		if self.sendingPacketsTriggered==True:

		    for neighbor in txNeighbors:
		    	    self._sixtop_txhousekeeping_per_neighbor(neighbor)
		
	        #=== rx-triggered housekeeping 
	        
	        # collect neighbors from which I have RX cells that is detected as collision cell
	        rxNeighbors = [cell['neighbor'] for ((ts,ch),cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision']]
	        
	        # remove duplicates
	        rxNeighbors = list(set(rxNeighbors))

	        for neighbor in rxNeighbors:
	            nowCells = self.numCellsFromNeighbors.get(neighbor,0)
	            assert nowCells == len([t for ((t,ch),c) in self.schedule.items() if c['dir']==self.DIR_RX and c['neighbor']==neighbor])
	            
	        # do some housekeeping for each neighbor
		#disable rx-triggered housekeeping for the moment
	        #for neighbor in rxNeighbors:
	            #self._sixtop_rxhousekeeping_per_neighbor(neighbor)
		          		   	
		#=== schedule next housekeeping

		self._sixtop_schedule_housekeeping()
    
    def _sixtop_txhousekeeping_per_neighbor(self,neighbor):
        '''
        For a particular neighbor, decide to relocate cells if needed.
        '''
        #===== step 1. collect statistics

        # pdr for each cell
        cell_pdr = []
        for ((ts,ch),cell) in self.schedule.items():
            if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX:
                # this is a TX cell to that neighbor
                # abort if not enough TX to calculate meaningful PDR
                if cell['numTx']<self.NUM_SUFFICIENT_TX:
                    continue
                
                # calculate pdr for that cell
                recentHistory = cell['history'][-self.NUM_MAX_HISTORY:]
                pdr = float(sum(recentHistory)) / float(len(recentHistory))

                # store result
                cell_pdr += [((ts,ch),pdr)]

        # pdr for the bundle as a whole
        bundleNumTx     = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        bundleNumTxAck  = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for cell in self.schedule.values() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX])
        if bundleNumTx<self.NUM_SUFFICIENT_TX:
            bundlePdr   = None
        else:
            bundlePdr   = float(bundleNumTxAck) / float(bundleNumTx)
        
        #===== step 2. relocate worst cell in bundle, if any
        # this step will identify the cell with the lowest PDR in the bundle.
        # If its PDR is self.sixtopPdrThreshold lower than the average of the bundle
        # this step will move that cell.
        
        relocation = False
        
        if cell_pdr:
            
            # identify the cell with worst pdr, and calculate the average
            worst_tsch   = None
            worst_pdr  = None

	    best_tsch = None	
	    best_pdr= None
            
            for ((ts,ch),pdr) in cell_pdr:
                if worst_pdr==None or pdr<worst_pdr:
                    worst_tsch  = (ts,ch)
                    worst_pdr = pdr

	    for ((ts,ch),pdr) in cell_pdr:
                if best_pdr==None or pdr>best_pdr:
                    best_tsch  = (ts,ch)
                    best_pdr = pdr
            
            assert worst_tsch!=None
            assert worst_pdr!=None
            
            # ave pdr for other cells
            othersNumTx      = sum([len(cell['history'][-self.NUM_MAX_HISTORY:]) for ((ts,ch),cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_tsch])
            othersNumTxAck   = sum([sum(cell['history'][-self.NUM_MAX_HISTORY:]) for ((ts,ch),cell) in self.schedule.items() if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX and ts != worst_tsch])           
            if othersNumTx<self.NUM_SUFFICIENT_TX:
                ave_pdr      = None
            else:
                ave_pdr      = float(othersNumTxAck) / float(othersNumTx)

            # relocate worst cell if "bad enough"
            if ave_pdr and worst_pdr<(ave_pdr/self.sixtopPdrThreshold):
                
                # log
                self._log(
                    self.INFO,
                    "[6top] relocating cell ts {0} to {1} (pdr={2:.3f} significantly worse than others {3})",
                    (worst_tsch,neighbor.id,worst_pdr,cell_pdr),
                )
	
                # relocate: remove the cell, in SF0 will add it in the future
	        #only relocate when TX cells are more than 2	 
                if len(self.getTxCells())>2 and self.sixtopState==self.IDLE and self.rplParentChangeOperationInCourse==False:
                    	    self._sixtop_removeCells_request_action(neighbor,1,[worst_tsch])

                            # update stats
                            self._stats_incrementMoteStats('topTxRelocatedCells')

                            # remember I relocated a cell for that bundle
                            relocation = True
                
        
        #===== step 3. relocate the complete bundle
        # this step only runs if the previous hasn't, and we were able to
        # calculate a bundle PDR.
        # This step verifies that the average PDR for the complete bundle is
        # expected, given the RSSI to that neighbor. If it's lower, this step
        # will move all cells in the bundle.
        
        bundleRelocation = False
        
        if (not relocation) and bundlePdr!=None:
            
            # calculate the theoretical PDR to that neighbor, using the measured RSSI
            rssi            = self.getRSSI(neighbor)
            theoPDR         = Topology.Topology.rssiToPdr(rssi)
            
            # relocate complete bundle if measured RSSI is significantly worse than theoretical
            if bundlePdr<(theoPDR/self.sixtopPdrThreshold):
		tsList=[]
                for ((ts,ch),_) in cell_pdr:
                    
                    # log
                    self._log(
                        self.INFO,
                        "[6top] relocating cell ts {0} to {1} (bundle pdr {2} << theoretical pdr {3})",
                        (ts,neighbor,bundlePdr,theoPDR),
                    )
		    tsList+=[(ts,ch)]

                # relocate: remove the cell, in SF0 will add it in the future
                if self.sixtopState==self.IDLE and self.rplParentChangeOperationInCourse==False:
            	    self._sixtop_removeCells_request_action(neighbor,len(tsList),tsList)

                    # update stats
                    self._stats_incrementMoteStats('topTxRelocatedCells')

                    # remember I relocated a cell for that bundle
                    relocation = True


    
    def _sixtop_rxhousekeeping_per_neighbor(self,neighbor):
        '''
        The RX node triggers a relocation when it has heard a packet
        from a neighbor it did not expect ('rxDetectedCollision')
        '''     
        
        rxCells = [((ts,ch),cell) for ((ts,ch),cell) in self.schedule.items() if cell['dir']==self.DIR_RX and cell['rxDetectedCollision'] and cell['neighbor']==neighbor]
   
        relocation = False
        for (ts,ch),cell in rxCells:
            
            # measure how many cells I have now from that child
            nowCells = self.numCellsFromNeighbors.get(neighbor,0)
           
            # relocate: add new first                   
            if self._sixtop_cell_reservation_request(neighbor,1,dir=self.DIR_RX) == 1:
            
                # relocate: remove old only when successfully added 
                if nowCells < self.numCellsFromNeighbors.get(neighbor,0):
                    if self.getTxCells():
			self._sixtop_removeCells_request_action(neighbor,len([(ts,ch)]),[(ts,ch)])
                        # remember I relocated a cell
                        relocation = True

        if relocation:
            # update stats
            self._stats_incrementMoteStats('topRxRelocatedCells')

    #send real 6p messages
    def _sixtop_cell_reservation_request_action(self,neighbor,numCells,direction=DIR_TX):
	''' tries to enqueue 6top ADD packet if the state is correct '''
	with self.dataLock:

		if self.sixtopState==self.IDLE:
		    assert self.timeoutAdd==0

		    self._sixtop_enqueueCMD_ADD(neighbor,numCells,direction)
		    return True
		#if not idle, increase the counter. If the counter reach the max value, reset the counter
		else:
		    if self.sixtopState==self.SIX_STATE_WAIT_ADDRESPONSE:
			self.timeoutAdd+=1
		        if self.timeoutAdd>max( random.randint(0,4)+math.ceil(self.settings.numMotes*(math.floor(self.maxWinShared/2))/(self.settings.numSHAREDCells*self.settings.numRadios*self.settings.otfHousekeepingPeriod)),self.TSCH_MAXTXRETRIES ): 
				self.timeoutAdd=0
				self.sixtopState=self.IDLE
				self.cellsPendingOperationType=None
				self.cellsPendingOperation=None
				self.responseType=None
				self.cellsPendingOperationNeigh=None
		    return False
    #send real 6p delete messages
    def _sixtop_removeCells_request_action(self,neighbor,numCellsToRemove,tsList):
	''' Removing cells. Always from child to parent, so dir=DIR_TX'''
	with self.dataLock:

		if self.sixtopState==self.IDLE:
		    assert self.timeoutDel==0
		    self._sixtop_enqueueCMD_DELETE(neighbor,numCellsToRemove,tsList)
		    return True
		#if not idle, increase the counter. If the counter reach the max value, reset the counter
		else:
		    if self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE:
			    self.timeoutDel+=1
			    if self.timeoutAdd>max( math.ceil(self.settings.numMotes*(math.floor(self.maxWinShared/2))/(self.settings.numSHAREDCells*self.settings.numRadios*self.settings.otfHousekeepingPeriod)),self.TSCH_MAXTXRETRIES ): 
				self.timeoutDel=0
				self.sixtopState=self.IDLE
				self.cellsPendingOperationType=None
				self.cellsPendingOperation=None
				self.responseType=None
				self.cellsPendingOperationNeigh=None
		    return False

    def _sixtop_enqueueCMD_ADD(self,neighbor,numCells,direction=DIR_TX):
        ''' enqueue 6top ADD packet into stack to be transmitted in a shared or dedicated cell '''
	with self.dataLock:

	    givenCells={}
	    givenCells_firstRound={}
	    givenCells_secondRound={} 

	    if self.settings.scheduler=='none':

		availableCells = []
		allCells = []
		
		#generate all cells: numChans X slotframe
		for x in range(0,self.settings.slotframeLength):
                    for y in range(0,self.settings.numChans):
		            cell=['0','0']
		            cell[0]=x
		            cell[1]=y
		            allCells.append(cell)

	
		availableCells=list(allCells)
	
		#get available cells
		for cell in allCells:
		    if cell in availableCells: #just in case the cells has been already removed
			#remove cells that are in my schedule
		        if (cell[0],cell[1]) in self.schedule.keys():
			    availableCells.remove([cell[0],cell[1]])

			    #check the radio capabilities				
			    cellsAtThatTs=[c for c in availableCells if c[0]==cell[0]]
			    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
			    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
				removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
				#if not enough radios, remove also these cells to avoid the neighbor choosing them
				for celltoremove in removeCandidates:
				    availableCells.remove([celltoremove[0],celltoremove[1]])

		#enqueue packet first round
		self.sixtopState=self.SIX_STATE_SENDING_REQUEST
		trafficType=self.SIXTOP_CMD_TRAFFIC	
		sixtopcmd=self.IANA_6TOP_CMD_ADD
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,0,numCells,direction,availableCells,self.settings.scheduler, neighbor],
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	 }# the payload is the id, the asn, the command, the auxiliar command, the number of cells and the direction, the candidate list in the sender the scheduler and the neighbor, usally the preferred parent

		#set states
		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
		assert self.cellsPendingOperationNeigh==None
		self.cellsPendingOperation=availableCells
		self.cellsPendingOperationType=direction
		self.cellsPendingOperationNeigh=neighbor
		
		isEnqueued = self._tsch_enqueue(newPacket,neighbor)
		
		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')
		    assert False	#usually the Shared queue is never full since RPL and 6top packages are enqueued smartly	
		
	    elif self.settings.scheduler=='cen':	#p-centralized without overlapping when saturation
		print "Not implemented yet"
		assert False
	    elif self.settings.scheduler=='opt2':	#p-centralized with overlapping when saturation

		availableCells = []
		allCells = []
		
		#generate all cells: numChans X slotframe
		for x in range(0,self.settings.slotframeLength):
                    for y in range(0,self.settings.numChans):
		            cell=['0','0']
		            cell[0]=x
		            cell[1]=y
		            allCells.append(cell)

		availableCells=list(allCells)

		#get available cells
		for cell in allCells:
		    if cell in availableCells: #just in case the cells has been already removed
			#remove cells that are in my schedule
		        if (cell[0],cell[1]) in self.schedule.keys():
			    availableCells.remove([cell[0],cell[1]])

			    #check the radio capabilities				
			    cellsAtThatTs=[c for c in availableCells if c[0]==cell[0]]
			    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
			    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
				removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
				#if not enough radios, remove also these cells to avoid the neighbor choosing them
				for celltoremove in removeCandidates:
				    availableCells.remove([celltoremove[0],celltoremove[1]])


		#enqueue packet first round
		self.sixtopState=self.SIX_STATE_SENDING_REQUEST
		trafficType=self.SIXTOP_CMD_TRAFFIC	
		sixtopcmd=self.IANA_6TOP_CMD_ADD
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,0,numCells,direction,availableCells,self.settings.scheduler, neighbor],
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	 }# the payload is the id, the asn, the command, the auxiliar command, the number of cells and the direction, the candidate list in the sender the scheduler and the neighbor, usally the preferred parent

		#set states
		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
		assert self.cellsPendingOperationNeigh==None
		self.cellsPendingOperation=availableCells
		self.cellsPendingOperationType=direction
		self.cellsPendingOperationNeigh=neighbor
		

		isEnqueued = self._tsch_enqueue(newPacket,neighbor)
		
		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')
		    assert False	#usually the Shared queue is never full since RPL and 6top packages are enqueued smartly	
          
	    elif self.settings.scheduler=='deBras':
		availableCells = []
		allCells = []

		#generate all cells: numChans X slotframe
		for x in range(0,self.settings.slotframeLength):
		    for y in range(0,self.settings.numChans):
			    cell=['0','0']
			    cell[0]=x
			    cell[1]=y
			    allCells.append(cell)

		availableCells=list(allCells)

		#remove the busy cells in my neighborhood
		for neigh in self.scheduleNeigborhood.keys():
		    if neigh != neighbor:
			for cell in self.scheduleNeigborhood[neigh].keys():
			    #avoid unnecessary checks 
			    if self.scheduleNeigborhood[neigh][(cell[0],cell[1])]['dir']!='SHARED':
				if [cell[0],cell[1]] in availableCells:		    
				    if self.scheduleNeigborhood[neigh][(cell[0],cell[1])]['dir']=='RX':
			    		availableCells.remove([cell[0],cell[1]])
				    else:
					if neighbor.getRSSI(neigh)+(-97-(-105)) >= self.minRssi:
						availableCells.remove([cell[0],cell[1]])

		#get available cells
		for cell in allCells:
		    if cell in availableCells: #just in case the cells has been already removed
			#remove cells that are in my schedule
		        if (cell[0],cell[1]) in self.schedule.keys():
			    availableCells.remove([cell[0],cell[1]])

			    #check the radio capabilities				
			    cellsAtThatTs=[c for c in availableCells if c[0]==cell[0]]
			    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
			    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
				removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
				#if not enough radios, remove also these cells to avoid the neighbor choosing them
				for celltoremove in removeCandidates:
				    availableCells.remove([celltoremove[0],celltoremove[1]])

		#enqueue packet first round
		self.sixtopState=self.SIX_STATE_SENDING_REQUEST
		trafficType=self.SIXTOP_CMD_TRAFFIC
		sixtopcmd=self.IANA_6TOP_CMD_ADD
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,0,numCells,direction,availableCells,self.settings.scheduler, neighbor], 
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	 }# the payload is the id, the asn, the command, the auxiliar command, the number of cells and the direction,the candidate list in the sender and the scheduler
		
		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
		assert self.cellsPendingOperationNeigh==None
		self.cellsPendingOperation=availableCells
		self.cellsPendingOperationType=direction
		self.cellsPendingOperationNeigh=neighbor
		

		isEnqueued = self._tsch_enqueue(newPacket,neighbor)
		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')
		    assert False	#usually the Shared queue is never full since RPL and 6top packages are enqueued smartly	
	    else:
		print "Unknown scheduler"
		assert False


    def _sixtop_enqueueCMD_DELETE(self,neighbor,numCells,tsList):
	''' enqueue 6top DELETE packet into stack to be transmitted in a shared or dedicated cell '''
	with self.dataLock:

		scheduleList = []
		if tsList==None:	#when no tsList is specified, remove the worst
		        
			# worst cell removing initialized by theoretical pdr
			for ((ts,ch),cell) in self.schedule.iteritems():
			    if cell['neighbor']==neighbor and cell['dir']==self.DIR_TX:
				cellPDR = (float(cell['numTxAck'])+(self.getPDR(neighbor)*self.NUM_SUFFICIENT_TX))/(cell['numTx']+self.NUM_SUFFICIENT_TX)
				scheduleList += [(ts,ch,cell['numTxAck'],cell['numTx'],cellPDR)]

			# introduce randomness in the cell list order
			random.shuffle(scheduleList)

			#use always worst cell
			# triggered only when worst cell selection is due
			# (cell list is sorted according to worst cell selection)
			scheduleListByPDR     = {}
			for tscell in scheduleList:
			    if not scheduleListByPDR.has_key(tscell[3]):
				scheduleListByPDR[tscell[3]]=[]
			    scheduleListByPDR[tscell[3]]+=[tscell]
			rssi                  = self.getRSSI(neighbor)
			theoPDR               = Topology.Topology.rssiToPdr(rssi)
			scheduleList          = []
			for pdr in sorted(scheduleListByPDR.keys()):
			    if pdr<theoPDR:
				scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2], reverse=True)
			    else:
				scheduleList += sorted(scheduleListByPDR[pdr], key=lambda x: x[2]) 
			    
			tsList=[]
			for tscell in scheduleList[:numCells]:
			    tsList += [(tscell[0],tscell[1])]		
		else:
			assert len(tsList)==numCells

		self.sixtopState=self.SIX_STATE_SENDING_REQUEST
		trafficType=self.SIXTOP_CMD_TRAFFIC
		sixtopcmd=self.IANA_6TOP_CMD_DELETE
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,0,tsList,neighbor], 
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	 }# the payload is the id, the asn, the command and the auxiliar command
		
		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
		assert self.cellsPendingOperationNeigh==None
		#this are needed to avoid in the future send packets in cells the node is about to delete
		self.cellsPendingOperation=tsList
		self.cellsPendingOperationType=self.DIR_TX
		self.cellsPendingOperationNeigh=neighbor
		
		isEnqueued = self._tsch_enqueue(newPacket,neighbor)

		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')
		    assert False	#usually the Shared queue is never full since RPL and 6top packages are enqueued smartly

    def _sixtop_enqueueCMD_ADD_Response(self,neighbor,direction,selectedCells,err):
	''' enqueue 6top ADD RESP packet into stack to be transmitted in a shared or dedicated cell '''
	with self.dataLock:
	        
		#enqueue packet for CMD ADD RESPONSE

		#check if duplicates:
		checkedCells={}
		i=0
		for a in selectedCells.values():
		    if a not in checkedCells.values():
			checkedCells[i]=a
			i+=1

		assert selectedCells==checkedCells
		assert self.sixtopState==self.SIX_STATE_REQUEST_RECEIVED
		assert self.responseType==None
		trafficType=self.SIXTOP_CMD_TRAFFIC
		if not err:
		    sixtopcmd=self.IANA_6TOP_RC_SUCCESS
		    self.responseType='ADDOK'
		else:
		    sixtopcmd=self.IANA_6TOP_RC_ERR
		    self.responseType='ADDERR'
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,self.IANA_6TOP_CMD_ADD,len(selectedCells),direction,selectedCells,neighbor], 
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	 }# the payload is the id, the asn, the command, the auxiliar command, the number of cells and the direction and the candidate list in the sender
		 #now the auxiliar command is used for refer the RC SUCCESS to CMD_ADD
	
		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
	        assert self.cellsPendingOperationNeigh==None							
		self.cellsPendingOperation=selectedCells
		self.cellsPendingOperationType=direction
		self.cellsPendingOperationNeigh=neighbor

		isEnqueued = self._tsch_enqueue(newPacket,neighbor)

		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')  
		    assert False	#usually the Shared queue is never full since RPL and 6top packages are enqueued smartly

    def _sixtop_enqueueCMD_DELETE_Response(self,neighbor,cellsForDeletion,err):
	''' enqueue 6top DEL RESP packet into stack to be transmitted in a shared or dedicated cell '''
	with self.dataLock:

		#enqueue packet for CMD ADD RESPONSE
		assert self.sixtopState==self.SIX_STATE_REQUEST_RECEIVED
		assert self.responseType==None
		trafficType=self.SIXTOP_CMD_TRAFFIC
		if not err:
		    sixtopcmd=self.IANA_6TOP_RC_SUCCESS
		    self.responseType='DELETEOK'
		else:
		    sixtopcmd=self.IANA_6TOP_RC_ERR
		    self.responseType='DELETEERR'
	    	newPacket = {
			'source':	  self,
			'dest':		  neighbor,
			'asn':            self.engine.getAsn(),
			'type':           trafficType,
			'payload':        [self.id,self.engine.getAsn(),sixtopcmd,self.IANA_6TOP_CMD_DELETE,cellsForDeletion,neighbor], 
			'retriesLeft':    self.TSCH_MAXTXRETRIES
	   	}# the payload is the id, the asn, the command, the auxiliar command, the number of cells and the direction and the candidate list in the sender
		#now the auxiliar command is used for refer the RC SUCCESS to CMD_DELETE

		assert self.cellsPendingOperation==None
		assert self.cellsPendingOperationType==None
		assert self.cellsPendingOperationNeigh==None
		self.cellsPendingOperation=cellsForDeletion
		self.cellsPendingOperationType=self.DIR_RX
		self.cellsPendingOperationNeigh=neighbor

		isEnqueued = self._tsch_enqueue(newPacket,neighbor)

		if not isEnqueued:
		    self._stats_incrementMoteStats('zixtopFailEnqueue')  
		    assert False

    def _sixtop_receiveCMD_ADD(self,type,smac,dmac,payload):
	'''Manage the ADD CMD'''

	with self.dataLock:
		    assert smac.id == payload[0]
		    assert dmac.id == self.id
		    assert payload[2] == self.IANA_6TOP_CMD_ADD
		    assert payload[3] == 0
		    numCells=payload[4]
#
		    dirNeighbor=payload[5]
		    neighbor=smac
		    # set direction of cells
		    if dirNeighbor == self.DIR_TX:
                	direction = self.DIR_RX
            	    else:
                	direction = self.DIR_TX

		    if self.settings.scheduler=='none':		#sf0	           
		   	#in the parent, numCells are tried to be reserved
			if self.settings.topology!='star':
		    		self.numRandomSelections+=1

		        #this are all my cells
             		candidatesNeigh=payload[6]
			candidates=list(candidatesNeigh)

	    		#remove my busy cells
	    		for cell in candidatesNeigh:
			    if cell in candidates: #just in case the cells has been already removed
				if (cell[0],cell[1]) in self.schedule.keys():
				    candidates.remove([cell[0],cell[1]])
				    cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
				    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
					removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
					for celltoremove in removeCandidates:
					    candidates.remove([celltoremove[0],celltoremove[1]])

				else:#check if there are other cells in the same ts but different channel
				    for i in range(0,self.settings.numChans):								
				        if (cell[0],i) in self.schedule.keys():
					    cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    	    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
					    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
						removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
						for celltoremove in removeCandidates:
						    candidates.remove([celltoremove[0],celltoremove[1]])


			#once the mote has the candidate cells, try to select some
		        selectedCells={}
		        if len(candidates) > 0:
		            			            
			    n=0
			    ranChosen=[]
			    while n<numCells:

				if len(candidates)>0:

					#select from candidates, a random one and remove it from the candidates
					selcel=random.sample(candidates, 1)[0]
					candidates.remove([selcel[0],selcel[1]])

					selectedCells[n]=selcel
					cellsAtThatTs=[c for c in candidates if c[0]==selcel[0]]
					
					#remove also the cells asociated with this cell according to the radios
					if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates:
						    candidates.remove([celltoremove[0],celltoremove[1]])	                       	
					n+=1
				else:
				        #not more candidates available
					break			
  
			    #enqueue response with an error
			    if numCells!=len(selectedCells):
				self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)

			    else:
			    	#instead of add cells, enqueue packet with RC_SUCCESS
			    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)

			else:
			    #if no cells available, enqueue packet with Error code
			    self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)	

		    elif self.settings.scheduler=='opt2':	#p-centralized

			candidatesNeigh=payload[6]
			candidates=list(candidatesNeigh)
			
	    		#remove my busy cells
	    		for cell in candidatesNeigh:
			    if cell in candidates: #just in case the cells has been already removed
				if (cell[0],cell[1]) in self.schedule.keys():
				    candidates.remove([cell[0],cell[1]])
				    cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
				    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):

					removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
					for celltoremove in removeCandidates:
						candidates.remove([celltoremove[0],celltoremove[1]])
				else:#check if there are other cells in the same ts but different channel
				    for i in range(0,self.settings.numChans):
									
				            if (cell[0],i) in self.schedule.keys():
						cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    		myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
						if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
							removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
							for celltoremove in removeCandidates:
								candidates.remove([celltoremove[0],celltoremove[1]])
			
		        #removing colliding cells using total knowledge
		        for mote in self.engine.motes:    
		            if mote != self and mote != neighbor:
				    if self.getRSSI(mote)+(-97-(-105)) >= mote.minRssi:		#avoid a mote interferes with me
					for cell in mote.schedule.keys():
					    if mote.schedule[(cell[0],cell[1])]['dir']!='SHARED':
						if [cell[0],cell[1]] in candidates:
						    if mote.schedule[(cell[0],cell[1])]['dir']=='TX':
							candidates.remove([cell[0],cell[1]])
						    else:					#avoid my nieghbor interferes with a mote
							if mote.getRSSI(neighbor)+(-97-(-105)) >= self.minRssi:
							    candidates.remove([cell[0],cell[1]])
				    if neighbor.getRSSI(mote)+(-97-(-105)) >= mote.minRssi:   #avoid my nieghbor interferes with a mote
		    		        for cell in mote.schedule.keys():
					    if mote.schedule[(cell[0],cell[1])]['dir']!='SHARED':
						if [cell[0],cell[1]] in candidates:
						    if mote.schedule[(cell[0],cell[1])]['dir']=='RX':
					    		candidates.remove([cell[0],cell[1]])
						    else:
							if self.getRSSI(mote)+(-97-(-105)) >= self.minRssi:
							    candidates.remove([cell[0],cell[1]])

		        #if I have cells, I try to assign them
		        selectedCells={}

		        if len(candidates) > 0:	                    
			    n=0
		
			    while n<numCells:
				if len(candidates)>0:
					#select from candidates, a random one and remove it from the candidates
					selcel=random.sample(candidates, 1)[0]
					candidates.remove([selcel[0],selcel[1]])

					#remove also the cells asociated with this cell according to the radios
					selectedCells[n]=selcel			
					cellsAtThatTs=[c for c in candidates if c[0]==selcel[0]]
					if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates:
							candidates.remove([celltoremove[0],celltoremove[1]])	                       	
					n+=1
				else:
					#not more candidates available
					break	
			    
         		    if numCells!=len(selectedCells): #node can try to overlap some of the neighbor's cells to acomplish the demand		
				
				#round two	
				candidatesNeigh=payload[6]
				candidates2=list(candidatesNeigh)

		    		#remove my busy cells
		    		for cell in candidatesNeigh:
				    if cell in candidates2: #just in case the cells has been already removed
					if (cell[0],cell[1]) in self.schedule.keys():
					    candidates2.remove([cell[0],cell[1]])
					    cellsAtThatTs=[c for c in candidates2 if c[0]==cell[0]]
					    if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])
					else:#check if there are other cells in the same ts but different channel
					    for i in range(0,self.settings.numChans):					
						    if (cell[0],i) in self.schedule.keys():	
							cellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
							if len(cellsAtThatTs)>(self.settings.numRadios-1):
							    if cell in candidates2: #just in case the cells has been already removed
							        candidates2.remove([cell[0],cell[1]])
				

				#remove from candidates the cell already available from first round
				#these are free of interference
				for c in selectedCells.values():
					candidates2.remove([c[0],c[1]])
					cellsAtThatTs=[ce for ce in candidates2 if ce[0]==c[0]]
					if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])

				#if I have cells, I try to assign them
				if len(candidates2) > 0:	                    
				    n=len(selectedCells)
			
				    while n<numCells:
					if len(candidates2)>0:
						#select from candidates, a random one and remove it from the candidates
						selcel=random.sample(candidates2, 1)[0]
						candidates2.remove([selcel[0],selcel[1]])

						#remove also the cells asociated with this cell according to the radios
						selectedCells[n]=selcel
						self.numRandomSelections+=1
						cellsAtThatTs=[c for c in candidates2 if c[0]==selcel[0]]
						if len(cellsAtThatTs)>(self.settings.numRadios-1):
							#num To Remove is len(cellsAtThatTs)-(self.settings.numRadios-1)
							removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
							for celltoremove in removeCandidates2:
								candidates2.remove([celltoremove[0],celltoremove[1]])	                       	
						n+=1
					else:
						#not more candidates available
						break

				    if numCells!=len(selectedCells):	#still the node don't get enough cells. even with overlapping
					#enqueue with errors
					self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)
				    else:

				    	#instead of add cells, enqueue packet with RC_SUCCESS but with some overlaping 
			    	    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
				
			        else:# cant be reached since there were already more than 0 cells, before removing the neighborhood limitation
				    #enqueue with errors
				    self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)


				    
				    
			    else:
				#instead of add cells, enqueue packet with RC_SUCCESS without error
			    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
			  
			#repeat operation for when there are not canidadtes
			else:
				#check if by overlapping in the neighbor's cells could get some extra cells
				candidatesNeigh=payload[6]
				candidates2=list(candidatesNeigh)

		    		#remove my busy cells
		    		for cell in candidatesNeigh:
				    if cell in candidates2: #just in case the cells has been already removed
					if (cell[0],cell[1]) in self.schedule.keys():
					    candidates2.remove([cell[0],cell[1]])
					    cellsAtThatTs=[c for c in candidates2 if c[0]==cell[0]]
					    if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])
					else:#check if there are other cells in the same ts but different channel
					    for i in range(0,self.settings.numChans):					
						    if (cell[0],i) in self.schedule.keys():	
							cellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
							if len(cellsAtThatTs)>(self.settings.numRadios-1):
							    if cell in candidates2: #just in case the cells has been already removed
							        candidates2.remove([cell[0],cell[1]])

				#if I have cells, I try to assign them
				if len(candidates2) > 0:	                    
				    n=len(selectedCells)
			
				    while n<numCells:
					if len(candidates2)>0:
						#select from candidates, a random one and remove it from the candidates
						selcel=random.sample(candidates2, 1)[0]
						candidates2.remove([selcel[0],selcel[1]])

						#remove also the cells asociated with this cell according to the radios
						selectedCells[n]=selcel
						self.numRandomSelections+=1
						cellsAtThatTs=[c for c in candidates2 if c[0]==selcel[0]]
						if len(cellsAtThatTs)>(self.settings.numRadios-1):
						    removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						    for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])	                       	
						n+=1
					else:
						#not more candidates available
						break

				    if numCells!=len(selectedCells): #still, enqueue packet with Error code
					self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)

				    else:
				   	#instead of add cells, enqueue packet with RC_SUCCESS but with some overlaping 
			    	    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
				
			        else:
			    	    #0 cells available, enqueue packet with Error code
			    	    self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)



		    elif self.settings.scheduler=='deBras':

			candidatesNeigh=payload[6]
			candidates=list(candidatesNeigh)

	    		#remove my busy cells
	    		for cell in candidatesNeigh:
			    if cell in candidates: #just in case the cells has been already removed
				if (cell[0],cell[1]) in self.schedule.keys():
				    candidates.remove([cell[0],cell[1]])
				    cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]

				    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):

					removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
					for celltoremove in removeCandidates:
						candidates.remove([celltoremove[0],celltoremove[1]])

				else:#check if there are other cells in the same ts but different channel
				    for i in range(0,self.settings.numChans):								
					if (cell[0],i) in self.schedule.keys():
					    cellsAtThatTs=[c for c in candidates if c[0]==cell[0]]
				    	    myCellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
					    if (len(cellsAtThatTs)+len(myCellsAtThatTs)-1)>(self.settings.numRadios-1):
						removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)+len(myCellsAtThatTs)-1-(self.settings.numRadios-1))
						for celltoremove in removeCandidates:
						    candidates.remove([celltoremove[0],celltoremove[1]])


			#remove the cells learned from the neighborhood  
		        for neigh in self.scheduleNeigborhood.keys():
			    if neigh != neighbor:
			        for cell in self.scheduleNeigborhood[neigh].keys():
				    if self.scheduleNeigborhood[neigh][(cell[0],cell[1])]['dir']!='SHARED':
				        if [cell[0],cell[1]] in candidates:
					    if self.scheduleNeigborhood[neigh][(cell[0],cell[1])]['dir']=='TX':
				    		candidates.remove([cell[0],cell[1]])
					    else:
						if self.scheduleNeigborhood[neigh][(cell[0],cell[1])]['neighbor'].getRSSI(self)+(-97-(-105)) >= self.minRssi:
						    candidates.remove([cell[0],cell[1]])



		        #if I have cells, I try to assign them
		        selectedCells={}

		        if len(candidates) > 0:	                    
			    n=0
				
			    while n<numCells:
				if len(candidates)>0:
					#select from candidates, a random one and remove it from the candidates
					selcel=random.sample(candidates, 1)[0]
					candidates.remove([selcel[0],selcel[1]])

					#remove also the cells asociated with this cell according to the radios
					selectedCells[n]=selcel
					cellsAtThatTs=[c for c in candidates if c[0]==selcel[0]]
					if len(cellsAtThatTs)>(self.settings.numRadios-1):
					    removeCandidates=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
					    for celltoremove in removeCandidates:
						candidates.remove([celltoremove[0],celltoremove[1]])	                       	
					n+=1
				else:
					#not more candidates available
					break	
			    
         		    if numCells!=len(selectedCells): #node can try to overlap some of the neighbor's cells		

				candidatesNeigh=payload[6]
				candidates2=list(candidatesNeigh)

		    		#remove my busy cells
		    		for cell in candidatesNeigh:
				    if cell in candidates2: #just in case the cells has been already removed
					if (cell[0],cell[1]) in self.schedule.keys():
					    candidates2.remove([cell[0],cell[1]])
					    cellsAtThatTs=[c for c in candidates2 if c[0]==cell[0]]
					    if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])
					else:#check if there are other cells in the same ts but different channel
					    for i in range(0,self.settings.numChans):					
						if (cell[0],i) in self.schedule.keys():	
						    cellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
						    if len(cellsAtThatTs)>(self.settings.numRadios-1):
							if cell in candidates2: #just in case the cells has been already removed
							    candidates2.remove([cell[0],cell[1]])

				#remove from candidates the cell already available from first round, these are free of overlaping
				for c in selectedCells.values():
					candidates2.remove([c[0],c[1]])
					cellsAtThatTs=[ce for ce in candidates2 if ce[0]==c[0]]
					if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:

							candidates2.remove([celltoremove[0],celltoremove[1]])


				#check which cells from my neighbors have been less updated and I will select them 
				#(maybe they are not in their schedule any more)
				lessUpdatedCells={}
				for c in candidates2:
				    for ne in self.scheduleNeigborhood.keys():
					for nc in self.scheduleNeigborhood[ne].keys():
					    if (c[0],c[1]) == (nc[0],nc[1]):
						lessUpdatedCells[c[0],c[1]]=self.scheduleNeigborhood[ne][(c[0],c[1])]['debrasFreshness']

				#if I have cells, I try to assign them
				if len(candidates2) > 0:	                    
				    n=len(selectedCells)
			
				    while n<numCells:
					if len(candidates2)>0:
						if len(lessUpdatedCells)>0:
							selcel=min(lessUpdatedCells, key=lessUpdatedCells.get)
							del lessUpdatedCells[(selcel[0],selcel[1])]
						else:
							selcel=random.sample(candidates2, 1)[0]
						candidates2.remove([selcel[0],selcel[1]])

						#remove also the cells asociated with this cell according to the radios
						selectedCells[n]=selcel
						cellsAtThatTs=[c for c in candidates2 if c[0]==selcel[0]]
						if len(cellsAtThatTs)>(self.settings.numRadios-1):
							removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
							for celltoremove in removeCandidates2:
								candidates2.remove([celltoremove[0],celltoremove[1]])	                       	
						n+=1
					else:
						#not more candidates available
						break

				    if numCells!=len(selectedCells):	#still the node don't get enough cells. even with overlapping
					self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)
				    else:
				    	#instead of add cells, enqueue packet with RC_SUCCESS but with some overlaping 
			    	    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
					#assert False

			        else:# cant be reached since there were already more than 0 cells, before removing the neighborhood limitation
				    self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)				 
			    else:
			    	#instead of add cells, enqueue packet with RC_SUCCESS
			    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
			  
			else:	#do it again for when 0 candidates

				#check if by overlapping in the neighbor's cells could get some extra cells
				candidatesNeigh=payload[6]
				candidates2=list(candidatesNeigh)

		    		#remove my busy cells
		    		for cell in candidatesNeigh:
				    if cell in candidates2: #just in case the cells has been already removed
					if (cell[0],cell[1]) in self.schedule.keys():
					    candidates2.remove([cell[0],cell[1]])
					    cellsAtThatTs=[c for c in candidates2 if c[0]==cell[0]]
					    if len(cellsAtThatTs)>(self.settings.numRadios-1):
						removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
						for celltoremove in removeCandidates2:
							candidates2.remove([celltoremove[0],celltoremove[1]])
					else:#check if there are other cells in the same ts but different channel
					    for i in range(0,self.settings.numChans):					
						    if (cell[0],i) in self.schedule.keys():	
							cellsAtThatTs=[c for c in self.schedule.keys() if c[0]==cell[0]]
							if len(cellsAtThatTs)>(self.settings.numRadios-1):
							    if cell in candidates2: #just in case the cells has been already removed
							        candidates2.remove([cell[0],cell[1]])



				#check which cells from my neighbors have been less updated and I will select them 
				#(maybe they are not in their schedule any more)
				lessUpdatedCells={}
				for c in candidates2:
				    for ne in self.scheduleNeigborhood.keys():
					for nc in self.scheduleNeigborhood[ne].keys():
					    if (c[0],c[1]) == (nc[0],nc[1]):
						lessUpdatedCells[c[0],c[1]]=self.scheduleNeigborhood[ne][(c[0],c[1])]['debrasFreshness']

				#if I have cells, I try to assign them
				if len(candidates2) > 0:	                    
				    n=len(selectedCells)
			
				    while n<numCells:
					if len(candidates2)>0:
						if len(lessUpdatedCells)>0:
							selcel=min(lessUpdatedCells, key=lessUpdatedCells.get)
							del lessUpdatedCells[(selcel[0],selcel[1])]
						else:
							selcel=random.sample(candidates2, 1)[0]

						#remove also the cells asociated with this cell according to the radios
						candidates2.remove([selcel[0],selcel[1]])
						selectedCells[n]=selcel
						self.numRandomSelections+=1
						cellsAtThatTs=[c for c in candidates2 if c[0]==selcel[0]]
						if len(cellsAtThatTs)>(self.settings.numRadios-1):
							#num To Remove is len(cellsAtThatTs)-(self.settings.numRadios-1)
							removeCandidates2=random.sample(cellsAtThatTs, len(cellsAtThatTs)-(self.settings.numRadios-1))
							for celltoremove in removeCandidates2:
								candidates2.remove([celltoremove[0],celltoremove[1]])	                       	
						n+=1
					else:
						#not more candidates available
						break

				    if numCells!=len(selectedCells):	#still the node don't get enough cells. even with overlapping
					self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)

				    else:
				   	#instead of add cells, enqueue packet with RC_SUCCESS but with some overlaping 
			    	    	self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=False)
				
			        else:
			    	    #0 cells available, enqueue packet with Error code
			    	    self._sixtop_enqueueCMD_ADD_Response(neighbor,direction,selectedCells,err=True)
		    else:
			print "Unkown scheduler"
			assert False

    def _sixtop_receiveCMD_ADD_response(self,type, smac,dmac, payload):
	''' receive 6top add packet from a shared or dedicated cell '''
	with self.dataLock:
		assert smac.id == payload[0]
		assert dmac.id == self.id
		assert payload[2] == self.IANA_6TOP_RC_SUCCESS or payload[2] == self.IANA_6TOP_RC_ERR
		self._stats_incrementMoteStats('zixtopRxCMDADDResp')

		#ADD resonse without errors
		if payload[2] == self.IANA_6TOP_RC_SUCCESS:

			    assert payload[3] == self.IANA_6TOP_CMD_ADD

			    cellList    = []
			    assert len(payload[6]) > 0 
			    givenCells=payload[6]
			    neighbor=smac
			    direction=payload[5]
			    if direction == self.DIR_TX:
				direction = self.DIR_RX
		    	    else:
				direction = self.DIR_TX		
			    for i,val in givenCells.iteritems():
				cellList         += [(val[0],val[1],direction)]
			    self._tsch_addCells(neighbor,cellList)

			    # update counters
			    if direction==self.DIR_TX:
				if neighbor not in self.numCellsToNeighbors:
				    self.numCellsToNeighbors[neighbor]     = 0
				self.numCellsToNeighbors[neighbor]        += len(givenCells)
			    else:
				if neighbor not in self.numCellsFromNeighbors:
				    self.numCellsFromNeighbors[neighbor]   = 0
				self.numCellsFromNeighbors[neighbor]      += len(givenCells)

			    self._stats_incrementMoteStats('zixtopSUCCESSResponseADD')

		#ADD resonse with errors
		elif payload[2] == self.IANA_6TOP_RC_ERR:


			if len(payload[6])>0:	#if error, 0 cells can be given
			    assert payload[3] == self.IANA_6TOP_CMD_ADD
			    
			    cellList    = []
			    givenCells=payload[6]
			    neighbor=smac
			    direction=payload[5]
			    if direction == self.DIR_TX:
				direction = self.DIR_RX
		    	    else:
				direction = self.DIR_TX		
			    for i,val in givenCells.iteritems():
				cellList         += [(val[0],val[1],direction)]
			    self._tsch_addCells(neighbor,cellList)

			    # update counters
			    if direction==self.DIR_TX:
				if neighbor not in self.numCellsToNeighbors:
				    self.numCellsToNeighbors[neighbor]     = 0
				self.numCellsToNeighbors[neighbor]        += len(givenCells)
			    else:
				if neighbor not in self.numCellsFromNeighbors:
				    self.numCellsFromNeighbors[neighbor]   = 0
				self.numCellsFromNeighbors[neighbor]      += len(givenCells)

			self.engine.saturationReached=True
		    	self._stats_incrementMoteStats('zixtopERRResponseADD')

		else:
			print "Unown 6top command "+str(payload[2])	
			assert False

		#reseting 6p states
	        self.cellsPendingOperationType=None
	        self.cellsPendingOperation=None
	        self.responseType=None
	        self.cellsPendingOperationNeigh=None			
		self.sixtopState=self.IDLE 
		self.timeoutAdd=0

	        if self.engine.turn==self.id:
		    self.engine.turn=self.engine.turn+1
		    for m in self.engine.motes:
			if m.id==self.engine.turn:
			    if m.otfTriggered==False:
				m.otfTriggered=True
				m._otf_schedule_housekeeping(firstOtf=True)

    def _sixtop_receiveCMD_DELETE(self,type,smac,dmac,payload):
	''' receive 6top del packet from a shared or dedicated cell '''
	with self.dataLock:
	    self._stats_incrementMoteStats('zixtopRxCMDDELETE')

	    for cell in payload[4]:

		    assert cell in self.schedule.keys()
	    self._sixtop_enqueueCMD_DELETE_Response(smac,payload[4],err=False)	
	    #there should not be errors when deleting
	
    def _sixtop_receiveCMD_DELETE_response(self,type, smac,dmac, payload):
	''' receive 6top del response packet from a shared or dedicated cell '''
	with self.dataLock:

		assert smac.id == payload[0]
		assert dmac.id == self.id
		assert payload[2] == self.IANA_6TOP_RC_SUCCESS or payload[2] == self.IANA_6TOP_RC_ERR
		tsList=payload[4]
		self._stats_incrementMoteStats('zixtopRxCMDDELETEResp')
		if payload[2] == self.IANA_6TOP_RC_SUCCESS:	#no error when deleting
		    assert payload[3] == self.IANA_6TOP_CMD_DELETE

		    self._stats_incrementMoteStats('zixtopSUCCESSResponseDel')
		    self._tsch_removeCells(
		        neighbor     = smac,
		        tsList       = tsList,
		    )
	    
		    self.numCellsToNeighbors[smac]       -= len(tsList)
		    assert self.numCellsToNeighbors[smac]>=0

		elif payload[2] == self.IANA_6TOP_RC_ERR:
		    self._stats_incrementMoteStats('zixtopERRResponseDel')
		    assert False	 #there should not be errors when deleting
		else:
		    print "Unown 6top command"	
		    assert False

		#reseting 6p states
	     	self.cellsPendingOperationType=None
	        self.cellsPendingOperation=None
	        self.responseType=None
	        self.cellsPendingOperationNeigh=None			
		self.sixtopState=self.IDLE 
		self.timeoutDel=0

    def _sixtop_receiveACK_ADD_RESPONSE(self):
	'''When confirming ADD Resp CMD'''
	with self.dataLock:
		
	    cellList    = []
	    
	    if self.responseType=='ADDOK':	#when ok, proceed to allocate	
		    assert len(self.cellsPendingOperation)>0
		    assert self.cellsPendingOperation!=None
	            assert self.cellsPendingOperationNeigh!=None
		    neighbor=self.cellsPendingOperationNeigh
		    givenCells=self.cellsPendingOperation
		    direction=self.cellsPendingOperationType	
		    for i,val in givenCells.iteritems():
			cellList         += [(val[0],val[1],direction)]
		    self._tsch_addCells(neighbor,cellList)

		    # update counters
		    if direction==self.DIR_TX:
			if neighbor not in self.numCellsToNeighbors:
			    self.numCellsToNeighbors[neighbor]     = 0
			self.numCellsToNeighbors[neighbor]        += len(givenCells)
		    else:
			if neighbor not in self.numCellsFromNeighbors:
			    self.numCellsFromNeighbors[neighbor]   = 0
			self.numCellsFromNeighbors[neighbor]      += len(givenCells)

	    elif self.responseType=='ADDERR':
		if len(self.cellsPendingOperation)>0:
		    assert self.cellsPendingOperation!=None
	            assert self.cellsPendingOperationNeigh!=None
		    neighbor=self.cellsPendingOperationNeigh
		    givenCells=self.cellsPendingOperation
		    direction=self.cellsPendingOperationType	
		    for i,val in givenCells.iteritems():
			cellList         += [(val[0],val[1],direction)]
		    self._tsch_addCells(neighbor,cellList)

		    # update counters
		    if direction==self.DIR_TX:
			if neighbor not in self.numCellsToNeighbors:
			    self.numCellsToNeighbors[neighbor]     = 0
			self.numCellsToNeighbors[neighbor]        += len(givenCells)
		    else:
			if neighbor not in self.numCellsFromNeighbors:
			    self.numCellsFromNeighbors[neighbor]   = 0
			self.numCellsFromNeighbors[neighbor]      += len(givenCells)

	    #reseting 6p states
	    self.sixtopState=self.IDLE    	
	    self.cellsPendingOperationType=None
	    self.cellsPendingOperation=None
	    self.cellsPendingOperationNeigh=None
	    self.responseType=None

    def _sixtop_receiveACK_DELETE_RESPONSE(self):
	'''When confirming DELETE Resp CMD'''
        with self.dataLock:

	    cellList    = []
	    
	    if self.responseType=='DELETEOK':
		    neighbor=self.cellsPendingOperationNeigh
	    	    assert len(self.cellsPendingOperation) != 0
		    assert self.cellsPendingOperation!=None
	            assert self.cellsPendingOperationNeigh!=None

		    self._tsch_removeCells(
		        neighbor     = self.cellsPendingOperationNeigh,
		        tsList       = self.cellsPendingOperation,
		    )
		
		    if self.numCellsFromNeighbors[self.cellsPendingOperationNeigh]:
                        self.numCellsFromNeighbors[self.cellsPendingOperationNeigh] -= len(self.cellsPendingOperation)
		    assert self.numCellsFromNeighbors[neighbor]>=0
	    else:
		assert False #when deletion, no error is expected

	    #reseting 6p states
	    self.sixtopState=self.IDLE    	
	    self.cellsPendingOperationType=None
	    self.cellsPendingOperation=None
	    self.cellsPendingOperationNeigh=None
	    self.responseType=None
 
    #===== tsch
    
    def _tsch_enqueue(self,packet,neigh):
		    
	    if packet['type']=='DATA':
		if not self.preferredParent:
		    # I don't have a route

		    # increment mote state
		    self._stats_incrementMoteStats('droppedNoRoute')

		    return False
	
		elif not self.getTxCells():
		    # I don't have any transmit cells
		    # here can be reached only for relay packets

		    # increment mote state
		    self._stats_incrementMoteStats('droppedNoTxCells')
		    return False
	
		elif len(self.txQueue)>=self.TSCH_QUEUE_SIZE:
		    # my TX queue is full

		    # update mote stats
		    self._stats_incrementMoteStats('droppedQueueFull')
		    return False
	
		else:
		    # all is good, enqueue packet	    

		    self.txQueue    += [packet]
		    return True


	    elif packet['type']=='RPLTRAFFIC':	#use always shared queue, there is always one shared cell
		if self.getSharedCells():
	    	    if len(self.txSharedQueue)==self.TSCH_QUEUE_SIZE:	#usually this queue should be never full
		        return False
	    	    else:
	    	    	self.txSharedQueue   += [packet]
	    	    	return True
		else:
		    print "No shared cells! Imposible!"
		    assert False 		   

	    elif packet['type']=='SIXTOP_CMD':
		assert neigh!=None
		
		if len(self.getTxCellsToNeighbor(neigh))<1:	
			if len(self.txSharedQueue)==self.TSCH_QUEUE_SIZE: #usually this queue should be never full
			    return False
		    	else:
		    	    self.txSharedQueue    += [packet]
		    	    return True
		else:# I have some dedicated cells, I will use them for 6top commands
			    #increasing incomming traffic to take in account the 6top message to the parent			    
			    sixtopPktAlreadyInQueue=False
			    for pkt in self.txQueue:
				if pkt['type'] =='SIXTOP_CMD':#do not enqueue
				    sixtopPktAlreadyInQueue=True
				    self._stats_incrementMoteStats('zixtopFailNeglectedEnqueue')
				    return True

			    #insert at the end
			    self.txSharedQueue    += [packet]

			    self._otf_incrementIncomingTraffic(self)
		    	    return True
	    else:
		print "Unknown traffic type "+str()
		assert False
    
    def _tsch_schedule_activeCell(self):
        
        asn        = self.engine.getAsn()
        tsCurrent  = asn%self.settings.slotframeLength

        # find closest active slot in schedule
        with self.dataLock:
            
            if not self.schedule:
                self.engine.removeEvent(uniqueTag=(self.id,'_tsch_action_activeCell'))
                return
            
            tsDiffMin             = None
            for ((ts,ch),cell) in self.schedule.items():
                if   ts==tsCurrent:
                    tsDiff        = self.settings.slotframeLength
                elif ts>tsCurrent:
                    tsDiff        = ts-tsCurrent
                elif ts<tsCurrent:
                    tsDiff        = (ts+self.settings.slotframeLength)-tsCurrent
                else:
                    raise SystemError()
                
                if (not tsDiffMin) or (tsDiffMin>tsDiff):
                    tsDiffMin     = tsDiff
                    
        # schedule at that ASN
        self.engine.scheduleAtAsn(
            asn         = asn+tsDiffMin,
            cb          = self._tsch_action_activeCell,
            uniqueTag   = (self.id,'_tsch_action_activeCell'),
            priority    = 0,
        )
        
    
    def _tsch_action_activeCell(self):
        '''
        active slot starts. Determine what todo, either RX or TX, use the propagation model to introduce
        interference and Rx packet drops.
        '''
        
        asn = self.engine.getAsn()
        ts  = asn%self.settings.slotframeLength
      
        with self.dataLock:
            
            self.pktToSend = []
         
            tss=[teses[0] for teses in self.schedule.keys()]

	    n=0
	    for (t1,c1) in self.schedule.keys():
	        if ts==t1:
		    n+=1
		
	    #important to avoid more simulatenous TX/RX than radios available 			
	    assert n<=self.settings.numRadios
            assert ts in tss
	                
            numberPacketSentInThisTs=0

            for i_ch in range(0,self.settings.numChans):
                if (ts,i_ch) in self.schedule.keys():
                    cell = self.schedule[(ts,i_ch)]
		   
                    if (cell['dir']==self.DIR_SHARED):
                        if asn > ((2*self.settings.slotframeLength)-1):	#avoid first 2 asn's
                                                 
			    if cell['isDebras']:	#this is DEBRAS cell
			    	if self.DEBRASALOHA==True:	#aloha mode
				    if self.numberOfWaitingsDeBras==0:
				    	assert cell['dir']==self.DIR_SHARED
				        self.numberOfWaitingsDeBras=random.randint(0,self.maxWin)
				        cell = self.schedule[(ts,i_ch)]

				        debras_payload={}
				        celdas=[(celda['ts'],celda['ch']) for celda in self.schedule.values() if celda['dir']!='SHARED']

				        random.shuffle(celdas)
				        payloadkeys=celdas[0:self.MAXCELLSDEBRASPAYLOAD]	#choose up to 36 cells

				        for c in payloadkeys:
					    debras_payload[(c[0],c[1])]=self.schedule[(c[0],c[1])]

				        if len(debras_payload.keys())>0:
				            packetToSend = {
							'source':	  self,
							'dest':		  None,
				                        'asn':            self.engine.getAsn(),
				                        'type':           self.SIXP_TYPE_MYSCHEDULE,
				                        'payload':        [self.id,self.engine.getAsn(),debras_payload], 	
				                        'retriesLeft':    1
				            }

				            self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED

					    #debras stats
				            self.deBrasTransmitted+=1                                
				            self.engine.deBrasTransmitted+=1  

				            self.propagation.startTx(
				                            channel   = cell['ch'],
				                            type      = packetToSend['type'],
				                            smac      = self,
				                            dmac      = self._myNeigbors(),
				                            payload   = packetToSend['payload'],
				                        )
				              
				            # log charge usage
				            self._logChargeConsumed(self.CHARGE_TxData_uC)


				    else:
					self.numberOfWaitingsDeBras=self.numberOfWaitingsDeBras-1
		                        self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED
		                        self.propagation.startRx(
		                                mote          = self,
						ts       = cell['ts'],
		                                channel       = cell['ch'],
		                        )
			        #DEBRAS TDMA mode
				else:

		                    if i_ch == (self.myBrCh) and ts==(self.myBrTs):    

		                        assert cell['dir']==self.DIR_SHARED
		                        
		                        if self.numberOfWaitingsDeBras==0:	#it's time to transmit something in this debras cell!
  
		                            self.numberOfWaitingsDeBras=self.maxWin-1

		                            assert i_ch == self.myBrCh
		                            assert ts == self.myBrTs
		                            cell = self.schedule[(ts,i_ch)]
		                            

					    debras_payload={}
					    celdas=[(celda['ts'],celda['ch']) for celda in self.schedule.values() if celda['dir']!='SHARED']

					    random.shuffle(celdas)
					    payloadkeys=celdas[0:self.MAXCELLSDEBRASPAYLOAD]	#36 cell limitation
					   
					    for c in payloadkeys:
						debras_payload[(c[0],c[1])]=self.schedule[(c[0],c[1])]

					    if len(debras_payload.keys())>0:
					            packetToSend = {
								'source':	  self,
								'dest':		  None,
					                        'asn':            self.engine.getAsn(),
					                        'type':           self.SIXP_TYPE_MYSCHEDULE,
					                        'payload':        [self.id,self.engine.getAsn(),debras_payload], 	
					                        'retriesLeft':    1
					            }

					            self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED

						    #debras stats
					            self.deBrasTransmitted+=1                                
					            self.engine.deBrasTransmitted+=1  

					            self.propagation.startTx(
					                            channel   = cell['ch'],
					                            type      = packetToSend['type'],
					                            smac      = self,
					                            dmac      = self._myNeigbors(),
					                            payload   = packetToSend['payload'],
					                        ) 
					            # log charge usage
					            self._logChargeConsumed(self.CHARGE_TxData_uC)   
              
		                        else:
		                            #if it is not my turn to transmit broadcast, I try to receive                                    
		                            self.numberOfWaitingsDeBras=self.numberOfWaitingsDeBras-1
		                            self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED
		                            self.propagation.startRx(
		                                mote          = self,
						ts       = cell['ts'],
		                                channel       = cell['ch'],
		                            )
		                        
		                    else:	#if it is not my turn neither my channel to transmit broadcast, I try to receive 
					self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED
		                        self.propagation.startRx(					    
		                            mote          = self,
					    ts       = cell['ts'],
		                            channel       = cell['ch'],
		                        )

                    	    else: 
				    #check if I have a rank and if I can use the SHARED cell, transmit
				    if self.numberOfWaitings==0 and self.rank!=None:		
				        self.numberOfWaitings=random.randint(0,self.maxWinShared)
				     
					#prepare the packet for sending
			                if self.txSharedQueue:
                                            if len(self.txSharedQueue) >= (numberPacketSentInThisTs+1):
                                    	        self.pktToSend.append(self.txSharedQueue[numberPacketSentInThisTs])

					#in this ts I could theoreticall send packets in different channels
                                        if len(self.pktToSend) >= (numberPacketSentInThisTs+1):
					    	self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED

						#prepare 6P message
						if self.pktToSend[numberPacketSentInThisTs]['type']=='SIXTOP_CMD':
						    
						    #unicast
						    self.pendingAck.append((ts,i_ch))
						    self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC)	#ack is expected
						    if self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_CMD_ADD:
						    	dest=self.pktToSend[numberPacketSentInThisTs]['payload'][8]
							self.sixtopState=self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE
							self._stats_incrementMoteStats('zixtopTxCMDADD')
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
							dest=self.pktToSend[numberPacketSentInThisTs]['payload'][5]
							self.sixtopState=self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE
							self._stats_incrementMoteStats('zixtopTxCMDDELETE')
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_RC_SUCCESS:
							
							if self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_ADD:
							    dest=self.pktToSend[numberPacketSentInThisTs]['payload'][7]
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDADDResp')
							elif self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_DELETE:
							    dest=self.pktToSend[numberPacketSentInThisTs]['payload'][5]
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDDELETEResp')
							else:
							    print "Unkown Auxiliar field. Unkown RC"
							    assert False
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_RC_ERR:
							
							if self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_ADD:
							    dest=self.pktToSend[numberPacketSentInThisTs]['payload'][7]
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDADDResp')
							elif self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_DELETE:
							    dest=self.pktToSend[numberPacketSentInThisTs]['payload'][5]
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDDELETEResp')
							else:
							    print "Unkown Auxiliar field. Unkown RC"
							    assert False
						    else:
							print "Unkown 6top command. Not propagating"
							assert False

						#prepare RPL DIO
						elif self.pktToSend[numberPacketSentInThisTs]['type']=='RPLTRAFFIC':
						    #broadcast
						    dest=self._myNeigbors()
						    self._stats_incrementMoteStats('rplTxDIO')
						    self._logChargeConsumed(self.CHARGE_TxData_uC)

						else:
							print "Unkown traffic type. Not propagating"
							assert False

					        assert cell['isDebras'] == False
						self.propagation.startTx(
					                            channel   = cell['ch'],
					                            type      = self.pktToSend[numberPacketSentInThisTs]['type'],
					                            smac      = self,
					                            dmac      = dest,
					                            payload   = self.pktToSend[numberPacketSentInThisTs]['payload'],
					        )

						if self.pktToSend[numberPacketSentInThisTs]['type']!='SIXTOP_CMD':
						    #not expecting ack for a DIO
						    self.txSharedQueue.remove(self.pktToSend[numberPacketSentInThisTs])
						    self.pktToSend.remove(self.pktToSend[0])
						numberPacketSentInThisTs=numberPacketSentInThisTs+1

					#if there are not packets, just receive
				        else:

	                                    self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED
	                                    self.propagation.startRx(
	                                         mote          = self,
						 ts       = cell['ts'],
	                                         channel       = cell['ch'],
	                                    )  
				    else:
					#receive in the shared cell
				        if self.numberOfWaitings!=0:
			    	            self.numberOfWaitings=self.numberOfWaitings-1
	                                self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_SHARED
	                                self.propagation.startRx(
	                                 	mote          = self,
						ts       = cell['ts'],
	                                 	channel       = cell['ch'],
	                                )

				
                
                    else:  #dedicated cell  
                        
                        assert self.schedule[(ts,i_ch)]
  
                        if  cell['dir']==self.DIR_RX:	#just receive
                            self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_RX
                            self.propagation.startRx(
                                mote          = self,
				ts       = cell['ts'],
                                channel       = cell['ch'],
                            ) 
                        elif cell['dir']==self.DIR_TX:

			    #get a packet from the queue
                            if len(self.txQueue) > (numberPacketSentInThisTs):
				for p in self.txQueue:	
				    #dmac will be the dest of the cell			    
				    if cell['neighbor']==p['dest']:
					if p not in self.pktToSend:
						self.pktToSend.append(p)
					break

                            # send packet
                            if bool(self.pktToSend) == True:
                                if len(self.pktToSend) > (numberPacketSentInThisTs):
                                        cell['numTx'] += 1
                                        self.schedule[(ts,i_ch)]['waitingfor']=self.DIR_TX                                    
                                        self.pendingAck.append((ts,i_ch))
					
					if self.pktToSend[numberPacketSentInThisTs]['type']=='SIXTOP_CMD':
	
						    dest=self.pktToSend[numberPacketSentInThisTs]['dest']
						    if self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_CMD_ADD:
						    	
							self.sixtopState=self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE
							self._stats_incrementMoteStats('zixtopTxCMDADD')
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
							
							self.sixtopState=self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE
							self._stats_incrementMoteStats('zixtopTxCMDDELETE')
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_RC_SUCCESS:
							
							if self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_ADD:
							   
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDADDResp')
							elif self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_DELETE:
							    
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDDELETEResp')
							else:
							    print "Unkown Auxiliar field. Unkown RC"
							    assert False
						    elif self.pktToSend[numberPacketSentInThisTs]['payload'][2]==self.IANA_6TOP_RC_ERR:
							if self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_ADD:
							    
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDADDResp')
							elif self.pktToSend[numberPacketSentInThisTs]['payload'][3]==self.IANA_6TOP_CMD_DELETE:
							    
							    self.sixtopState=self.SIX_STATE_WAIT_RESPONSE_SENDDONE
							    self._stats_incrementMoteStats('zixtopTxCMDDELETEResp')
							else:
							    print "Unkown Auxiliar field. Unkown RC"
							    assert False
						    else:
							print "Unkown 6top command. Not propagating"
							assert False
					else:	
						assert self.pktToSend[numberPacketSentInThisTs]['type']=='DATA'
						self.numTransmissions += 1

						dest=self.schedule[(ts,i_ch)]['neighbor']

						avoidTX=False
						if self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE:
						    #for avoiding send packets in cells I am about to delete
						    if self.cellsPendingOperationType==self.DIR_TX and (ts,i_ch) in self.cellsPendingOperation:
							for c in self.schedule.keys():
							    
							    if c[0]==ts:								
								if self.schedule[(c[0],c[1])]['dir']==self.DIR_SHARED:

									avoidTX=True
					        if avoidTX:
					            continue
					#transmit!
                                        self.propagation.startTx(
                                            channel   = cell['ch'],
                                            type      = self.pktToSend[numberPacketSentInThisTs]['type'],
                                            smac      = self,
                                            dmac      = dest,
                                            payload   = self.pktToSend[numberPacketSentInThisTs]['payload'],
                                        )
                                                                    
                                        # log charge usage
                                        self._logChargeConsumed(self.CHARGE_TxDataRxAck_uC) #ack is expected
                                        numberPacketSentInThisTs=numberPacketSentInThisTs+1

                    self._tsch_schedule_activeCell()
    
    def _tsch_addCells(self,neighbor,cellList):
        ''' adds cell(s) to the schedule '''
        
        with self.dataLock:
            for cell in cellList:
                
                self.schedule[(cell[0],cell[1])] = {
                    'ts':                        cell[0],
                    'ch':                        cell[1],
                    'dir':                       cell[2],
                    'neighbor':                  neighbor,
		    'isDebras':                  False,
		    'debrasFreshness':           0,
                    'numTx':                     0,
                    'busy':                      0,
                    'numTxAck':                  0,
                    'sharedCell_id':             None,
                    'numRx':                     0,
                    'history':                   [],
                    'waitingfor':                None,
                    'rxDetectedCollision':       False,
                    'debug_canbeInterfered':     [],                      # [debug] shows schedule collision that can be interfered with minRssi or larger level 
                    'debug_interference':        [],                      # [debug] shows an interference packet with minRssi or larger level 
                    'debug_lockInterference':    [],                      # [debug] shows locking on the interference packet
                    'debug_cellCreatedAsn':      self.engine.getAsn(),    # [debug]
                }

            self._tsch_schedule_activeCell()
            
            
    def _tsch_removeCells(self,neighbor,tsList):
        ''' removes cell(s) from the schedule '''

        with self.dataLock:
            # log
            self._log(
                self.INFO,
                "[tsch] remove timeslots={0} with {1}",
                (tsList,neighbor.id),
            )
            for ts,ch in tsList:
                assert (ts,ch) in self.schedule.keys()
               	assert self.schedule[(ts,ch)]['dir']!=self.DIR_SHARED
                del self.schedule[(ts,ch)]
            self._tsch_schedule_activeCell()
    

    #===== radio
    
    def radio_txDone(self,isACKed,isNACKed,txtype):
        '''end of tx slot, when ACKs are expected in the TX side'''

        asn   = self.engine.getAsn()
        ts    = asn%self.settings.slotframeLength
        
        with self.dataLock:
            tss=[row[0] for row in self.schedule.keys()]
            assert ts in tss
    
            i_ch=0
            for i_ch in range(self.settings.numChans):
                if (ts,i_ch) in self.schedule.keys():        

		    if (ts,i_ch) in self.pendingAck:
                    	assert self.schedule[(ts,i_ch)]['waitingfor']==self.DIR_TX or txtype=='SIXTOP_CMD'
		        #shared cells are not expected to receive an ack unless is a sixtop packet

			if txtype!='SIXTOP_CMD':
                            assert self.schedule[(ts,i_ch)]['dir']==self.DIR_TX
                        

                        if isACKed:
                            # ACK received

                            # update schedule stats
                            self.schedule[(ts,i_ch)]['numTxAck'] += 1
                            
                            # update history
                            self.schedule[(ts,i_ch)]['history'] += [1]
                            
                            # time correction
                            if self.schedule[(ts,i_ch)]['neighbor'] == self.preferredParent:
                                self.timeCorrectedSlot = asn
                            
                            #update the 6p states and perform the CMD 
			    if txtype=='SIXTOP_CMD':
				if self.sixtopState==self.SIX_STATE_WAIT_ADDREQUEST_SENDDONE:
					self.sixtopState=self.SIX_STATE_WAIT_ADDRESPONSE
					
				elif self.sixtopState==self.SIX_STATE_WAIT_DELETEREQUEST_SENDDONE:
                                	self.sixtopState=self.SIX_STATE_WAIT_DELETERESPONSE
					
				elif self.sixtopState==self.SIX_STATE_WAIT_RESPONSE_SENDDONE:
					
					if self.responseType == None:
						assert False
					if self.responseType=='ADDOK' or self.responseType=='ADDERR':
						self._sixtop_receiveACK_ADD_RESPONSE()	#confirm and reserve
					else:
						self._sixtop_receiveACK_DELETE_RESPONSE() #confirm and delete
				else:
					print "Received an ACK in a wrong state"
					assert False

			    # remove packet from queue			    
			    if self.schedule[(ts,i_ch)]['dir']==self.DIR_SHARED:	#shared queue
				self.txSharedQueue.remove(self.txSharedQueue[0]) 
				self.pktToSend.remove(self.pktToSend[0])

			    else:							#normal queue
				if self.pktToSend[0] not in self.txQueue:
					assert False
			    	self.txQueue.remove(self.pktToSend[0])
				if txtype!='SIXTOP_CMD': 
					self._stats_logQueueDelay(asn-self.pktToSend[0]['asn'])
				self.pktToSend.remove(self.pktToSend[0])

			    #mote is not expecting this ack anymore
                            self.pendingAck.remove((ts,i_ch))

			    
			    self.schedule[(ts,i_ch)]['waitingfor']=None
			    return #these return are necessary in case we have to receive acks in several channels

                        elif isNACKed:  #i.e. when fails in enqueue packet  or when 6top CMD is received when busy     
                            # NACK received

                            # update schedule stats as if it were successfully transmitted
                            self.schedule[(ts,i_ch)]['numTxAck'] += 1

                            # update history
                            self.schedule[(ts,i_ch)]['history'] += [1]
                            
                            # time correction
                            if self.schedule[(ts,i_ch)]['neighbor'] == self.preferredParent:
                                self.timeCorrectedSlot = asn

                            if self.schedule[(ts,i_ch)]['dir']==self.DIR_SHARED:
	   			    if txtype=='SIXTOP_CMD':
					if self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_CMD_ADD:
						self._stats_incrementMoteStats('zixtopTxCMDADDNacked') 
					elif self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
						self._stats_incrementMoteStats('zixtopTxCMDDELETENacked') 
					else:
						print "Unkown 6top command"
						assert False
			    else:
				    if txtype=='SIXTOP_CMD':
					if self.txQueue[0]['payload'][2]==self.IANA_6TOP_CMD_ADD:
						self._stats_incrementMoteStats('zixtopTxCMDADDNacked') 
					elif self.txQueue[0]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
						self._stats_incrementMoteStats('zixtopTxCMDDELETENacked') 
					else:
						print "Unkown 6top command"
						assert False
               
			    if self.schedule[(ts,i_ch)]['dir']==self.DIR_SHARED:                                                           
	                            # remove packet from queue
	                            self.txSharedQueue.remove(self.txSharedQueue[0])

				    #reset 6p states for when 6p in shared cell
	               		    self.sixtopState=self.IDLE
	    			    self.cellsPendingOperationType=None
	    			    self.cellsPendingOperation=None
				    self.responseType=None
				    self.cellsPendingOperationNeigh=None

			    else:
				    if txtype=='SIXTOP_CMD':
					    #reset 6p states for when 6p in dedicated cell
					    self.sixtopState=self.IDLE
		    			    self.cellsPendingOperationType=None
		    			    self.cellsPendingOperation=None
					    self.responseType=None
					    self.cellsPendingOperationNeigh=None
	                            self.txQueue.remove(self.pktToSend[0])
	                            self.pktToSend.remove(self.pktToSend[0])
	
			    self.pendingAck.remove((ts,i_ch))

                            # end of radio activity, not waiting for anything
                            self.schedule[(ts,i_ch)]['waitingfor']=None
			    return

                        else:
                            # neither ACK nor NACK received
                            
                            # update history
                            self.schedule[(ts,i_ch)]['history'] += [0]

			    #decrease counters
			    if self.schedule[(ts,i_ch)]['dir']==self.DIR_SHARED:
				if self.txSharedQueue[0]['retriesLeft'] > 0:
                                    self.txSharedQueue[0]['retriesLeft'] -= 1
			    else:
                            	i = self.txQueue.index(self.pktToSend[0])

				if self.txQueue[i]['retriesLeft'] > 0:
                                    self.txQueue[i]['retriesLeft'] -= 1			    

                    
                            if self.schedule[(ts,i_ch)]['dir']==self.DIR_SHARED:
		                    # drop packet if retried too many times
		                    if self.txSharedQueue[0]['retriesLeft'] == 0:
					    if txtype=='SIXTOP_CMD':
						#have not recived an ACK/NACK for 5 times. Dropping 6top packet
						#reset 6p states
				       		self.sixtopState=self.IDLE
			    			self.cellsPendingOperationType=None
			    			self.cellsPendingOperation=None
						self.responseType=None
						self.cellsPendingOperationNeigh=None
						if self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_CMD_ADD:
							self._stats_incrementMoteStats('zixtopTxCMDADDDropped') 
						elif self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
							self._stats_incrementMoteStats('zixtopTxCMDDELETEDropped') 
						elif self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_RC_SUCCESS:
							self._stats_incrementMoteStats('zixtopTxCMDRCSUCCESSDropped')
						elif self.txSharedQueue[0]['payload'][2]==self.IANA_6TOP_RC_ERR:
							self._stats_incrementMoteStats('zixtopTxCMDRCERRDropped')
						else:
							print "Unkown 6top command"
							assert False   
					    else:
							print "No data packets with ack are allowed in shared cells yet"
							assert False   						                                                            
		                            # remove packet from queue
		                            self.txSharedQueue.remove(self.txSharedQueue[0])				
					    
			    else:
		                    # drop packet if retried too many times
		                    if self.txQueue[i]['retriesLeft'] == 0: 
					if txtype!='SIXTOP_CMD':                                             
		                            self._stats_incrementMoteStats('droppedMacRetries')
					else:
						#reset 6p states
						self.sixtopState=self.IDLE
			    			self.cellsPendingOperationType=None
			    			self.cellsPendingOperation=None
						self.responseType=None
						self.cellsPendingOperationNeigh=None
						if self.txQueue[i]['payload'][2]==self.IANA_6TOP_CMD_ADD:
							self._stats_incrementMoteStats('zixtopTxCMDADDDropped') 
						elif self.txQueue[i]['payload'][2]==self.IANA_6TOP_CMD_DELETE:
							self._stats_incrementMoteStats('zixtopTxCMDDELETEDropped') 
						elif self.txQueue[i]['payload'][2]==self.IANA_6TOP_RC_SUCCESS:
							self._stats_incrementMoteStats('zixtopTxCMDRCSUCCESSDropped')
						elif self.txQueue[i]['payload'][2]==self.IANA_6TOP_RC_ERR:
							self._stats_incrementMoteStats('zixtopTxCMDRCERRDropped')
						else:
							print "Unkown 6top command"
							assert False                                                                
		                        # remove packet from queue
		                        self.txQueue.remove(self.pktToSend[0])

			    if txtype=='DATA':
				self.pktToSend.remove(self.pktToSend[0])   #remove from the list of packet expected to be received at this ts     
			    elif txtype=='SIXTOP_CMD':		
		                self.pktToSend.remove(self.pktToSend[0])   #remove from the list of packet expected to be received at this ts     
					
			    self.pendingAck.remove((ts,i_ch))
                            # end of radio activity, not waiting for anything
                            self.schedule[(ts,i_ch)]['waitingfor']=None
			    return
		       
    def radio_rxDone(self,type=None,smac=None,dmac=None,payload=None,channel=None):
        '''end of RX radio activity'''
        
        asn   = self.engine.getAsn()
        ts    = asn%self.settings.slotframeLength
   
        with self.dataLock:

            if type=='DEBRAS' or type=='RPLTRAFFIC' or type=='SIXTOP_CMD':

                if self.schedule.has_key((ts,channel)) and self.schedule[(ts,channel)]['waitingfor']==self.DIR_SHARED and self.schedule[(ts,channel)]['dir']==self.DIR_SHARED:

                    if smac:
                        # I received a packet
			if type=='RPLTRAFFIC':# RPL
				self._logChargeConsumed(self.CHARGE_RxData_uC)
				
	    			if (self.settings.topology!='star') or (self.settings.topology=='star' and self.rank==None):
					self._rpl_action_receiveDIO(type, smac, payload)		        
                    		(isACKed, isNACKed) = (False, False)

                   		self.schedule[(ts,channel)]['waitingfor']=None

                    		return isACKed, isNACKed
			elif type=='DEBRAS':

		                # log charge usage
		                self._logChargeConsumed(self.CHARGE_RxData_uC)                        

		                scheduleOfNeigbor=payload[2]

		                self._updateSchedule(scheduleOfNeigbor,smac)                      
		                self.deBrasReceived+=1                        
		                self.engine.deBrasReceived+=1

		                (isACKed, isNACKed) = (False, False) # it is not acked, however it is not important since in txdone is not checked
		                self.schedule[(ts,channel)]['waitingfor']=None
		                return isACKed, isNACKed
			elif type=='SIXTOP_CMD':
			    #request comands
			    if dmac.id==self.id:
				
				if payload[2]==self.IANA_6TOP_CMD_ADD:
					self._stats_incrementMoteStats('zixtopRxCMDADD')

					if self.sixtopState==self.IDLE:
						#mote is idle, send ACK
						self.sixtopState=self.SIX_STATE_REQUEST_RECEIVED
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None

						return isACKed, isNACKed
					else:
						#mote is busy, send NACK
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)

						self._stats_incrementMoteStats('zixtopdroppedBusy') 
						(isACKed, isNACKed) = (False, True)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed

				elif payload[2]==self.IANA_6TOP_CMD_DELETE:
					if self.sixtopState==self.IDLE:
						#mote is idle, send ACK
						self.sixtopState=self.SIX_STATE_REQUEST_RECEIVED
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						#mote is busy, send NACK
						self._stats_incrementMoteStats('zixtopdroppedBusy') 
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						(isACKed, isNACKed) = (False, True)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed

				#response comands
				elif payload[2]==self.IANA_6TOP_RC_SUCCESS:
					if payload[3]==self.IANA_6TOP_CMD_ADD:

						assert self.sixtopState==self.SIX_STATE_WAIT_ADDRESPONSE	#if dmac is me, then check the state!
						
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					if payload[3]==self.IANA_6TOP_CMD_DELETE:

						assert self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						print "Unknown Auxiliar code. Wrong Return Code"
						assert False
	
				elif payload[2]==self.IANA_6TOP_RC_ERR:
					if payload[3]==self.IANA_6TOP_CMD_ADD:

						assert self.sixtopState==self.SIX_STATE_WAIT_ADDRESPONSE	#if dmac is me, then check the state!
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					if payload[3]==self.IANA_6TOP_CMD_DELETE:
						assert self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						print "Unknown Auxiliar code. Wrong Return Code"
						assert False
				else:
				    print "Unknown 6TOP Command"
				    assert False
			else:
				print "Unkown packet"
				assert False
                    else:
                        # this was an idle listen
    
                        # log charge usage
                        self._logChargeConsumed(self.CHARGE_Idle_uC)
                                
                        (isACKed, isNACKed) = (False, False)


                        self.schedule[(ts,channel)]['waitingfor']=None
                        return isACKed, isNACKed

	    #this is a dedicated cell
            if type=='DATA' or type=='SIXTOP_CMD':               
                for i_ch in range(0,self.settings.numChans):                
                    if (ts,i_ch) in self.schedule.keys() and self.schedule[(ts,channel)]['dir']!=self.DIR_SHARED:           
                        assert self.schedule[(ts,channel)]['dir']!=self.DIR_SHARED 
                        if self.schedule[(ts,i_ch)]['waitingfor']==self.DIR_RX:
                            assert self.schedule[(ts,i_ch)]['dir']==self.DIR_RX
                            assert self.schedule[(ts,i_ch)]['waitingfor']==self.DIR_RX

                            if smac:	#it is for this mote
				if type=='SIXTOP_CMD':
				    assert dmac.id==self.id	#check packet destination of a dedicated cell	

				    #request comands		
				    if payload[2]==self.IANA_6TOP_CMD_ADD:
					if self.sixtopState==self.IDLE:

						self.sixtopState=self.SIX_STATE_REQUEST_RECEIVED
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD(type, smac,dmac, payload)
						
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._stats_incrementMoteStats('zixtopdroppedBusy') 
						(isACKed, isNACKed) = (False, True)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
				    elif payload[2]==self.IANA_6TOP_CMD_DELETE:
					if self.sixtopState==self.IDLE:
						self.sixtopState=self.SIX_STATE_REQUEST_RECEIVED
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._stats_incrementMoteStats('zixtopdroppedBusy') 
						(isACKed, isNACKed) = (False, True)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed

				    #response comands
				    elif payload[2]==self.IANA_6TOP_RC_SUCCESS:
					if payload[3]==self.IANA_6TOP_CMD_ADD:

						assert self.sixtopState==self.SIX_STATE_WAIT_ADDRESPONSE
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					if payload[3]==self.IANA_6TOP_CMD_DELETE:

						assert self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						print "Unknown Auxiliar code. Wrong Return Code"
						#return nack
						assert False
	
				    elif payload[2]==self.IANA_6TOP_RC_ERR:
					if payload[3]==self.IANA_6TOP_CMD_ADD:
						
						assert self.sixtopState==self.SIX_STATE_WAIT_ADDRESPONSE	#if dmac is me, then chek the state!
						
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_ADD_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					if payload[3]==self.IANA_6TOP_CMD_DELETE:
						assert self.sixtopState==self.SIX_STATE_WAIT_DELETERESPONSE
						self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)
						self._sixtop_receiveCMD_DELETE_response(type, smac,dmac, payload)
						(isACKed, isNACKed) = (True, False)
						self.schedule[(ts,channel)]['waitingfor']=None
						return isACKed, isNACKed
					else:
						print "Unknown Auxiliar code. Wrong Return Code"
						#return nack
						assert False
				    else:
				        print "Unknown 6TOP Command"
				        assert False
				else:# I received a data packet

				    # log charge usage
				    self._logChargeConsumed(self.CHARGE_RxDataTxAck_uC)	
		
				    # update schedule stats
				    self.schedule[(ts,i_ch)]['numRx'] += 1
				    self.numReceptions += 1

				    if self.dagRoot:
					    # receiving packet (at DAG root)
					    
					    # update mote stats
					    self._stats_incrementMoteStats('appReachesDagroot')
					    
					    #emunicio
					    self.numPacketReceived=self.numPacketReceived+1 

					    #within the experiment
					    if (self.engine.asn > (self.engine.experimentInitTime*self.settings.slotframeLength)):
						self.probeNumPacketReceived=self.probeNumPacketReceived+1

					    # calculate end-to-end latency
					    self._stats_logLatencyStat(asn-payload[1])
					    
					    # log the number of hops
					    self._stats_logHopsStat(payload[2])

					    (isACKed, isNACKed) = (True, False)
					    
					    self.schedule[(ts,i_ch)]['waitingfor']=None
					    return isACKed, isNACKed
				    else:
					    # relaying packet
					    self._otf_incrementIncomingTraffic(smac)
					    
					    # update the number of hops
					    newPayload     = copy.deepcopy(payload)
					    newPayload[2] += 1
					    
					    # create packet
					    relayPacket = {
						'source':	  self,
						'dest':		  self.preferredParent,
						'asn':         asn,
						'type':        type,
						'payload':     newPayload,
						'retriesLeft': self.TSCH_MAXTXRETRIES
					    }
					    
					    # enqueue packet in TSCH queue
					    isEnqueued = self._tsch_enqueue(relayPacket,None)
					    
					    if isEnqueued:
					
						# update mote stats
						self._stats_incrementMoteStats('appRelayed')
					
						(isACKed, isNACKed) = (True, False)
					
						self.schedule[(ts,i_ch)]['waitingfor']=None                                
						return isACKed, isNACKed
					    else:
						#send nack even if there is no empty space
						self._stats_incrementMoteStats('droppedAppFailedEnqueue')
						(isACKed, isNACKed) = (False, True)

						self.schedule[(ts,i_ch)]['waitingfor']=None
						return isACKed, isNACKed
                            else:
                                # this was an idle listen
  
                                # log charge usage
                                self._logChargeConsumed(self.CHARGE_Idle_uC)
                                
                                (isACKed, isNACKed) = (False, False)
                    
                                self.schedule[(ts,i_ch)]['waitingfor']=None
                                return isACKed, isNACKed    
            
            else:#packet illegible I dont know type neither cell 

		 #always count charge
		 self._logChargeConsumed(self.CHARGE_Idle_uC)
               
                 (isACKed, isNACKed) = (False, False)  
                 
                 #if the broadcast packet has failed, we still can wait for a correct broadcast in other channel
                 if (ts,channel) in self.schedule.keys():    
                     self.schedule[(ts,channel)]['waitingfor']=None

                 return isACKed, isNACKed
           
    
    #===== wireless
    
    def setPDR(self,neighbor,pdr):
        ''' sets the pdr to that neighbor'''
        with self.dataLock:
            self.PDR[neighbor] = pdr
    
    def getPDR(self,neighbor):
        ''' returns the pdr to that neighbor'''
        with self.dataLock:
            return self.PDR[neighbor]
    
    def setRSSI(self,neighbor,rssi):
        ''' sets the RSSI to that neighbor'''
        with self.dataLock:
            self.RSSI[neighbor] = rssi
    
    def getRSSI(self,neighbor):
        ''' returns the RSSI to that neighbor'''
        with self.dataLock:
            #emunicio
            if neighbor==self:
                return self.minRssi
            else:
                return self.RSSI[neighbor]
    
    def _estimateETX(self,neighbor):
        
        with self.dataLock:
            
            # set initial values for numTx and numTxAck assuming PDR is exactly estimated
	    if neighbor not in self.firstPDR.keys():
            	pdr                   = self.getPDR(neighbor)
		self.firstPDR[neighbor]=pdr
	    else:
		pdr=self.firstPDR[neighbor]
		
            numTx                 = self.NUM_SUFFICIENT_TX
            numTxAck              = math.floor(pdr*numTx)

            for (_,cell) in self.schedule.items():
                if (cell['neighbor'] == neighbor) and (cell['dir'] == self.DIR_TX):  #ok shared cell broadcast is not taken in account
                    numTx        += cell['numTx']
                    numTxAck     += cell['numTxAck']
            
            # abort if about to divide by 0
            if not numTxAck:
                return

            # calculate ETX           
            etx = float(numTx)/float(numTxAck)
	    
            return etx
    
    def _myNeigbors(self):
        return [n for n in self.PDR.keys() if self.PDR[n]>0]

    #===== clock
   
    def clock_getOffsetToDagRoot(self):
        ''' calculate time offset compared to the DAGroot '''
        
        asn                  = self.engine.getAsn()
        offset               = 0.0
        child                = self
        parent               = self.preferredParent

        if parent!=None:    
		i=0
		while True:
		    if self.timeCorrectedSlot==None:
			break
		    secSinceSync     = (asn-child.timeCorrectedSlot)*self.settings.slotDuration  # sec
		    # FIXME: for ppm, should we not /10^6?
		    relDrift         = child.drift - parent.drift                                # ppm
		    offset          += relDrift * secSinceSync                                   # us
		    if parent.dagRoot:
		        break
		    else:
		        child        = parent
		        parent       = child.preferredParent
	else:
		offset=0
        
        return offset
        
    #===== topology

    def recalculateNumHopsToRoot(self):
        ''' calculate hops from mote to root '''
        child                = self
        parent               = self.preferredParent
        i=0
        
        while True:
            i=i+1
            if i>30:
		break
            if parent.dagRoot:
                break
            else:
                child        = parent
                parent       = child.preferredParent
             
        return i

    #===== location
    
    def setLocation(self,x,y):
        with self.dataLock:
            self.x = x
            self.y = y
    
    def getLocation(self):
        with self.dataLock:
            return (self.x,self.y)
    
    #==== init
    
    def boot(self):
	''' boot the mote '''
	if self.dagRoot:
		self.engine.dagRoot=self
	
	#all nodes try to schedule a DIO
        self._rpl_schedule_sendDIO(firstDIO=True)
	
        # OTF
        self._otf_resetInboundTrafficCounters()

        # 6top
        if not self.settings.sixtopNoHousekeeping:
            self._sixtop_schedule_housekeeping()

	#tsch

	# set initial SHARED cells
        self._schedule_setInitialCells()

	# set initial DEBRAS cells
	if self.settings.scheduler=='deBras':
	    self._schedule_setDeBrasInitialCells()
	
	self.maxWinShared=5						#set initial win size for shared cells
	self.numberOfWaitings=random.randint(0,self.maxWinShared)	#set initial delay for use the shared cells

	#record the visible neighbours at boot
	self.numVisibleNeighbors=self._myNeigbors()

        self._tsch_schedule_activeCell()


    def _logChargeConsumed(self,charge):
        with self.dataLock:
            self.chargeConsumed  += charge
	    
    
    #======================== private =========================================
    
    #===== getters
   
    def getTxCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['dir']==self.DIR_TX]
    
    def getRxCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['dir']==self.DIR_RX]
    def getRxCellsToNeighbor(self,neighbor):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['dir']==self.DIR_RX and c['neighbor']==neighbor]
    def getTxCellsToNeighbor(self,neighbor):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['dir']==self.DIR_TX and c['neighbor']==neighbor]

    def getSharedCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['dir']==self.DIR_SHARED]
    def getDeBrasSharedCells(self):
        with self.dataLock:
            return [(ts,c['ch'],c['neighbor']) for ((ts,ch),c) in self.schedule.items() if c['isDebras']==True]

    #===== stats
    
    # mote state
    def getMoteStats(self):

        # gather statistics
        with self.dataLock:
            returnVal = copy.deepcopy(self.motestats)
            returnVal['numTxCells']         = len(self.getTxCells())
            returnVal['numRxCells']         = len(self.getRxCells())
            returnVal['aveQueueDelay']      = self._stats_getAveQueueDelay()
            returnVal['aveLatency']         = self._stats_getAveLatency()
            returnVal['aveHopsPackets']     = self.hopsToRoot
            returnVal['aveHops']            = self._stats_getAveHops()
            returnVal['probableCollisions'] = self._stats_getRadioStats('probableCollisions')            
            returnVal['txQueueFill']        = len(self.txQueue)
            returnVal['PKTTX']              = self.numPacketSent
            returnVal['PKTRX']              = self.numPacketReceived
            returnVal['numReqCells']        = self.numReqCells
            returnVal['chargeConsumed']     = self.chargeConsumed
            returnVal['numTx']              =self.numTransmissions
            returnVal['numRx']              =self.numReceptions
            returnVal['thReqCells']         =self.threq
            returnVal['txBroadcast']     = self.deBrasTransmitted #individual stats of debras messages received
            returnVal['rxBroadcast']     = self.deBrasReceived	#individual stats of debras messages received
	    returnVal['numRandomSelections']     = self.numRandomSelections
	    returnVal['zznumMotesJoined']     = self.moteJoined # number nodes joined in the network
	    returnVal['zznumMotesWithTxCells']     = self.moteWithTxCells # number nodes joined in the network
	    returnVal['zznumMotesSending']     = self.moteSending # number nodes sending packets in the network

        # reset the statistics
        self._stats_resetMoteStats()
        self._stats_resetQueueStats()
        self._stats_resetLatencyStats()
        self._stats_resetHopsStats()
        self._stats_resetRadioStats()
        
        return returnVal

    def _stats_resetMoteStats(self):
        with self.dataLock:
            self.motestats = {
                # app
                'appGenerated':            0,   # number of packets app layer generated
                'appRelayed':              0,   # number of packets relayed
                'appReachesDagroot':       0,   # number of packets received at the DAGroot
                'droppedAppFailedEnqueue': 0,   # dropped packets because app failed enqueue them
                # queue
                'droppedQueueFull':        0,   # dropped packets because queue is full
		 # 6top
                'zixtopTxCMDADD':          		0,   # number of TX'ed CMD_ADD
		'zixtopTxCMDADDNacked':         	0,   # number of TX nacked CMD_ADD
		'zixtopTxCMDADDDropped':         	0,   # number of TX dropped CMD_ADD
		'zixtopRxCMDADD':          		0,   # number of RX'ed CMD_ADD
		'zixtopTxCMDDELETE':          		0,   # number of TX'ed CMD_DELETE
		'zixtopTxCMDDELETENacked':         	0,   # number of TX nacked CMD_DELETE
		'zixtopTxCMDDELETEDropped':          	0,   # number of TX dropped CMD_ADD
		'zixtopRxCMDDELETE':          		0,   # number of RX'ed CMD_DELETE
                'zixtopTxCMDADDResp':          		0,   # number of TX'ed CMD_ADDResp
		'zixtopRxCMDADDResp':          		0,   # number of RX'ed CMD_ADDResp
		'zixtopTxCMDDELETEResp':          	0,   # number of TX'ed CMD_DELETEResp
		'zixtopRxCMDDELETEResp':          	0,   # number of RX'ed CMD_DELETEResp
		'zixtopTxCMDRCSUCCESSDropped':          0,   # number of TX CMD_SUCCESS dropped
		'zixtopTxCMDRCERRDropped':          	0,   # number of TX CMD_ERR dropped
		'zixtopdroppedBusy':			0, #number of drops due to the mote is busy
		'zixtopFailEnqueue':			0,
		'zixtopFailNeglectedEnqueue':		0,# packet is not enqueued becaouse there is already one 6top packet in the queue
	        'zixtopERRResponseADD':			0,# response add err
		'zixtopSUCCESSResponseADD':		0,# response add success
		'zixtopERRResponseDel':			0,# response del err
		'zixtopSUCCESSResponseDel':		0,# response del success

                # rpl
                'rplTxDIO':                0,   # number of TX'ed DIOs
                'rplRxDIO':                0,   # number of RX'ed DIOs
                'rplChurnPrefParent':      0,   # number of time the mote changes preferred parent
                'rplChurnRank':            0,   # number of time the mote changes rank
                'rplChurnParentSet':       0,   # number of time the mote changes parent set
                'droppedNoRoute':          0,   # packets dropped because no route (no preferred parent)
                # otf
                'otfAdd':                  0,   # OTF adds some cells
                'otfRemove':               0,   # OTF removes some cells
                'droppedNoTxCells':        0,   # packets dropped because no TX cells
                # 6top
                'topTxRelocatedCells':     0,   # number of time tx-triggered 6top relocates a single cell
                'topTxRelocatedBundles':   0,   # number of time tx-triggered 6top relocates a bundle
                'topRxRelocatedCells':     0,   # number of time rx-triggered 6top relocates a single cell
                # tsch
                'droppedMacRetries':       	 0,   # packets dropped because more than TSCH_MAXTXRETRIES MAC retries
                'numReqCells':                   0,	
                'thReqCells':                    0,
                'cellsNotGiven':            	 0,
            }
    
    def _stats_incrementMoteStats(self,name):
        with self.dataLock:
            self.motestats[name] += 1
 
    # cell stats   
    def getCellStats(self,ts_p,ch_p):
        ''' retrieves cell stats '''
        
        returnVal = None
        with self.dataLock:
            for ((ts,ch),cell) in self.schedule.items():
                if ts==ts_p and cell['ch']==ch_p:
                    returnVal = {
                        'dir':            cell['dir'],
                        'neighbor':       cell['neighbor'].id,
                        'numTx':          cell['numTx'],
                        'numTxAck':       cell['numTxAck'],
                        'numRx':          cell['numRx'],
                    }
                    break
        return returnVal
    
    # queue stats

    def _stats_logQueueDelay(self,delay):
        with self.dataLock:
            self.queuestats['delay'] += [delay]
    
    def _stats_getAveQueueDelay(self):
        d = self.queuestats['delay']
        return float(sum(d))/len(d) if len(d)>0 else 0
    
    def _stats_resetQueueStats(self):
        with self.dataLock:
            self.queuestats = {
                'delay':               [],
            }
    
    # latency stats

    def _stats_logLatencyStat(self,latency):
        with self.dataLock:
            self.packetLatencies += [latency]
    
    def _stats_getAveLatency(self):
        with self.dataLock:
            d = self.packetLatencies
            return float(sum(d))/float(len(d)) if len(d)>0 else 0
    
    def _stats_resetLatencyStats(self):
        with self.dataLock:
            self.packetLatencies = []
    
    # hops stats
    
    def _stats_logHopsStat(self,hops):
        with self.dataLock:
            self.packetHops += [hops]
    
    def _stats_getAveHops(self):
        with self.dataLock:
            d = self.packetHops
            return float(sum(d))/float(len(d)) if len(d)>0 else 0
    
    def _stats_resetHopsStats(self):
        with self.dataLock:
            self.packetHops = []
    
    # radio stats
    
    def stats_incrementRadioStats(self,name):
        with self.dataLock:
            self.radiostats[name] += 1
    
    def _stats_getRadioStats(self,name):
        return self.radiostats[name]
    
    def _stats_resetRadioStats(self):
        with self.dataLock:
            self.radiostats = {
                'probableCollisions':      0,   # number of packets that can collide with another packets 
            }
    
    #===== log
    
    def _log(self,severity,template,params=()):
        
        if   severity==self.DEBUG:
            if not log.isEnabledFor(logging.DEBUG):
                return
            logfunc = log.debug
        elif severity==self.INFO:
            if not log.isEnabledFor(logging.INFO):
                return
            logfunc = log.info
        elif severity==self.WARNING:
            if not log.isEnabledFor(logging.WARNING):
                return
            logfunc = log.warning
        elif severity==self.ERROR:
            if not log.isEnabledFor(logging.ERROR):
                return
            logfunc = log.error
        else:
            raise NotImplementedError()
        
        output  = []
        output += ['[ASN={0:>6} id={1:>4}] '.format(self.engine.getAsn(),self.id)]
        output += [template.format(*params)]
        output  = ''.join(output)
        logfunc(output)
        
    def _log_printEndResults(self):
	''' update the counters in SimEngine '''

        with self.dataLock:
	    
            self.engine.totalRx=self.engine.totalRx+self.numReceptions
            self.engine.totalTx=self.engine.totalTx+self.numTransmissions		
	    self.engine.packetsSentToRoot=self.probePacketsGenerated+self.engine.packetsSentToRoot
	    self.engine.olGeneratedToRoot=(self.probePacketsGenerated/self.engine.timeElapsedFlow)+self.engine.olGeneratedToRoot
	    self.engine.pkprobeGeneratedToRoot=self.probePacketsGenerated+self.engine.pkprobeGeneratedToRoot	    	
	    self.engine.pkprobeReceivedInRoot=self.probeNumPacketReceived+self.engine.pkprobeReceivedInRoot
	    self.engine.packetReceivedInRoot=self.probeNumPacketReceived+self.engine.packetReceivedInRoot
	    self.engine.thReceivedInRoot=(self.probeNumPacketReceived/self.engine.timeElapsedFlow)+self.engine.thReceivedInRoot


    def _schedule_setInitialCells(self):
        ''' Set initial SHARED cells '''
        with self.dataLock:

	    sharedCell_id=0  

	    self.scheduleNeigborhood={}

            for j_ch in range(0,self.settings.numChans):
                ts_b=0
		for n in range(0,self.settings.numSHAREDCells):
                    cell = (ts_b,j_ch)
		    if sharedCell_id < self.settings.numMotes:
			    self.schedule[(ts_b,j_ch)] = {
		                'ts':                        ts_b,
		                'ch':                        j_ch,
		                'dir':                       self.DIR_SHARED,
		                'neighbor':                  None,	#no neighbor predefined since is shared cell
				'isDebras':		     False,
				'debrasFreshness':           0,
		                'numTx':                     0,
		                'numTxAck':                  0,
		                'sharedCell_id':             sharedCell_id,
		                'numRx':                     0,
		                'busy':                      0,
		                'history':                   [],
		                'waitingfor':                None,
		                'rxDetectedCollision':       False,
		                'debug_canbeInterfered':     [],                      # [debug] shows schedule collision that can be interfered with minRssi or larger level 
		                'debug_interference':        [],                      # [debug] shows an interference packet with minRssi or larger level 
		                'debug_lockInterference':    [],                      # [debug] shows locking on the interference packet
		                'debug_cellCreatedAsn':      self.engine.getAsn(),    # [debug]
		            }

			    ts_b+=int(self.settings.slotframeLength/self.settings.numSHAREDCells)
			    sharedCell_id+=1
                if j_ch>=(self.settings.numRadios-1):	#only set shared cells in the same ts up the num radios available
			break

    def _schedule_setDeBrasInitialCells(self):
        ''' Set initial DEBRAS cells '''
        with self.dataLock:
	    sharedCell_id=0  

	    self.scheduleNeigborhood={}

            for j_ch in range(0,self.settings.numChans):
            	
                ts_b=1
                for n in range(0,self.settings.numDeBraSCells):
                    cell = (ts_b,j_ch)

		    if sharedCell_id < self.settings.numMotes:

			    if (ts_b,j_ch) in self.schedule.keys():
		    		assert False

			    self.schedule[(ts_b,j_ch)] = {
		                'ts':                        ts_b,
		                'ch':                        j_ch,
		                'dir':                       self.DIR_SHARED,
		                'neighbor':                  None,	#no neighbor predefined since is shared cell
				'isDebras':		     True,
				'debrasFreshness':           0,
		                'numTx':                     0,
		                'numTxAck':                  0,
		                'sharedCell_id':              sharedCell_id,
		                'numRx':                     0,
		                'busy':                      0,
		                'history':                   [],
		                'waitingfor':                None,
		                'rxDetectedCollision':       False,
		                'debug_canbeInterfered':     [],                      # [debug] shows schedule collision that can be interfered with minRssi or larger level 
		                'debug_interference':        [],                      # [debug] shows an interference packet with minRssi or larger level 
		                'debug_lockInterference':    [],                      # [debug] shows locking on the interference packet
		                'debug_cellCreatedAsn':      self.engine.getAsn(),    # [debug]
		            }

			    ts_b+=int(self.settings.slotframeLength/self.settings.numDeBraSCells)
			    sharedCell_id+=1
                if j_ch>=(self.settings.numRadios-1):	#only set shared cells in the same ts up the num radios available
			break
             
            if self.settings.numDeBraSCells!=0:            
                self.maxWin=math.ceil((float(self.settings.numMotes)/(sharedCell_id)))
                self.numberOfWaitingsDeBras= int((self.id/(sharedCell_id)))                    
                value=[(ts,c['ch']) for ((ts,ch),c) in self.schedule.items() if c['isDebras']==True if c['sharedCell_id']==(self.id % (sharedCell_id))]
                self.myBrTs=value[0][0]
                self.myBrCh=value[0][1]
		
		#if aloha version, set random waitings and set the win size
		if self.DEBRASALOHA==True:
			self.maxWin=int(((self.settings.numMotes-1)/(sharedCell_id))) *2
			self.numberOfWaitingsDeBras=random.randint(0,self.maxWin)

    def _updateSchedule(self,scheduleOfNeighbor,neighbor):
    	''' Actions when received a DEBRAS message '''
        with self.dataLock:

		if neighbor not in self.scheduleNeigborhood.keys():
		    self.scheduleNeigborhood[neighbor]={}

		for c in scheduleOfNeighbor.values():
		    if (c['ts'],c['ch']) not in self.scheduleNeigborhood[neighbor].keys():

			self.scheduleNeigborhood[neighbor][(c['ts'],c['ch'])]=c
			self.scheduleNeigborhood[neighbor][(c['ts'],c['ch'])]['debrasFreshness']+=1

			#perform relocation in case of collision detected:
			if (c['ts'],c['ch']) in self.schedule.keys() and self.schedule[(c['ts'],c['ch'])]['dir']=='TX' and self.schedule[(c['ts'],c['ch'])]['neighbor']!=self.preferredParent:

				if len(self.getTxCells())>1 and self.sixtopState==self.IDLE and self.rplParentChangeOperationInCourse==False:

				    if len(self.engine.nodeHasTxCellsTime.keys())==self.settings.numMotes-1:
		            	        self._sixtop_removeCells_request_action(self.preferredParent,1,[(c['ts'],c['ch'])])

		                        # update stats
		                        self._stats_incrementMoteStats('topTxRelocatedCells')
