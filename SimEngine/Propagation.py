#!/usr/bin/python
'''
\brief Wireless propagation model.

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
log = logging.getLogger('Propagation')
log.setLevel(logging.DEBUG)
log.addHandler(NullHandler())

#============================ imports =========================================

import threading
import random
import math
import operator

import Topology
import SimSettings
import SimEngine

#============================ defines =========================================

#============================ body ============================================

class Propagation(object):
    
    #===== start singleton
    _instance      = None
    _init          = False
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Propagation,cls).__new__(cls, *args, **kwargs)
        return cls._instance
    #===== end singleton
    
    def __init__(self):
        
        #===== start singleton
        # don't re-initialize an instance (needed because singleton)
        if self._init:
            return
        self._init = True
        #===== end singleton

        # store params
        self.settings                  = SimSettings.SimSettings()
        self.engine                    = SimEngine.SimEngine()
        
        # variables
        self.dataLock                  = threading.Lock()
        self.receivers                 = [] # motes with radios currently listening
        self.transmissions             = [] # ongoing transmissions
	
        # schedule propagation task
        self._schedule_propagate()
    
    def destroy(self):
        self._instance                 = None
        self._init                     = False
    
    #======================== public ==========================================
    
    #===== communication
    
    def startRx(self,mote,ts,channel):
        ''' add a mote as listener on a channel'''
        with self.dataLock:
            self.receivers += [{
                'mote':                mote,
		'ts':                ts,
                'channel':             channel,
            }]
    
    def startTx(self,channel,type,smac,dmac,payload):
        ''' add a mote as using a channel for tx'''
        with self.dataLock:
            self.transmissions  += [{
                'channel':             channel,
                'type':                type,
                'smac':                smac,
                'dmac':                dmac,
                'payload':             payload,
            }]
    
    def propagate(self):
        ''' Simulate the propagation of pkts in a slot. '''
        
        with self.dataLock:
            
            asn   = self.engine.getAsn()
            ts    = asn%self.settings.slotframeLength
            
            arrivalTime = {}
       
            # store arrival times of transmitted packets 
            for transmission in self.transmissions:
                if transmission['smac'].id != 0:
                    arrivalTime[transmission['smac']] = transmission['smac'].clock_getOffsetToDagRoot()
                else:
                    arrivalTime[transmission['smac']] = self.engine.getAsn()

            #sor transmissions by channel
            sortedTransmissionByChannel=[]
            for i in range(0,self.settings.numChans):
                for transmission in self.transmissions:
                    if transmission['channel']==i:
                        sortedTransmissionByChannel.append(transmission)


	    nodesRxAtThisCell=[]
	    txcell=[None,None]
            for transmission in sortedTransmissionByChannel:
                
                i           = 0 # index of a receiver
                isACKed     = False
                isNACKed    = False

		#get nodes receiving at this channel
		if txcell == [None,None]:
			txcell[0]=ts
			txcell[1]=transmission['channel']
			nodesRxAtThisCell=[]
			for recv in self.receivers:
			    if recv['ts']==ts and recv['channel']==transmission['channel']:
			        nodesRxAtThisCell.append(recv)			
		else:
		    if txcell[1]!=transmission['channel']:
			assert txcell[0]==ts
			txcell[1]=transmission['channel']
			nodesRxAtThisCell=[]
			for recv in self.receivers:
			    if recv['ts']==ts and recv['channel']==transmission['channel']:
			        nodesRxAtThisCell.append(recv)

                if 'DEBRAS' == transmission['type'] or 'RPLTRAFFIC' == transmission['type']:
		    while i<len(self.receivers):

                        if self.receivers[i] in nodesRxAtThisCell:

                            interferers = [t['smac'] for t in self.transmissions if (t!=transmission) and (t['channel']==transmission['channel'])]
                            
                            interferenceFlag = 0
                            for itfr in interferers:
                               
                                if self.receivers[i]['mote'].getRSSI(itfr) +(-97-(-105))>self.receivers[i]['mote'].minRssi:
                                    interferenceFlag = 1

                            if interferenceFlag:
                                transmission['smac'].stats_incrementRadioStats('probableCollisions') 

                            lockOn = transmission['smac']
                            for itfr in interferers:

                                if arrivalTime[itfr] < arrivalTime[lockOn] and self.receivers[i]['mote'].getRSSI(itfr)+(-97-(-105))>self.receivers[i]['mote'].minRssi:
                                    # lock on interference
                                    lockOn = itfr
                            
                            if lockOn == transmission['smac']:
                                # mote locked in the current signal

                                transmission['smac'].schedule[(ts,transmission['channel'])]['debug_lockInterference'] += [0] # debug only
                                
                                # calculate pdr, including interference
                                sinr  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],interferers,True)
                                pdr   = self._computePdrFromSINR(sinr, self.receivers[i]['mote'])
                                
				# calculate pdr, without interference to check later if there have been a collision
				sinr2  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],[],False)
				pdr2   = self._computePdrFromSINR(sinr2, self.receivers[i]['mote'])
                                                                  
                                # pick a random number
                                failure = random.random() 

                                if pdr>=failure:
        
                                    isACKed, isNACKed = self.receivers[i]['mote'].radio_rxDone(
                                        type       = transmission['type'],
                                        smac       = transmission['smac'],
                                        dmac       = self.receivers[i]['mote'],
                                        payload    = transmission['payload'],
                                        channel    = transmission['channel']
					
                                    )
				    #Desired debras/RPL packet is received, this node can not receive more in this cell
				    nodesRxAtThisCell.remove(self.receivers[i])
                                    
                                else:

                                    if interferenceFlag:
					   if pdr2 > 0.0:
						#this a collision, this node can not receive more
						nodesRxAtThisCell.remove(self.receivers[i])
						#collisions in shared cells are not considered for the stats

			      #only for debug
##                            else:                                                            
                                # mote locked in an interfering signal

#                                # for debug
#                                transmission['smac'].schedule[(ts,transmission['channel'])]['debug_lockInterference'] += [1]
#                                
#                                # receive the interference as if it's a desired packet
#                                interferers.remove(lockOn)
#                                pseudo_interferers = interferers + [transmission['smac']]
#                                
#                                # calculate SINR where locked interference and other signals are considered S and I+N respectively
#                                pseudo_sinr  = self._computeSINR(lockOn,self.receivers[i]['mote'],pseudo_interferers)
#                                pseudo_pdr   = self._computePdrFromSINR(pseudo_sinr, self.receivers[i]['mote'])
                               
#                                # pick a random number
#                                failure = random.random()
                                
                                #if pseudo_pdr>=failure:
                                    # success to receive the interference and realize collision
                                    #self.receivers[i]['mote'].schedule[(ts,transmission['channel'])]['rxDetectedCollision'] = True


                        i += 1

		#propagation of a 6P packet
		elif 'SIXTOP_CMD' == transmission['type']:

                    #for recv in self.receivers:
		    while i<len(self.receivers):

			if self.receivers[i] in nodesRxAtThisCell:

                            # this packet is destined for this mote
                            if self.receivers[i]['mote']==transmission['dmac']:
                         
                                if not self.settings.noInterference:
                                    #================ with interference ===========
                                     
                                    interferers = [t['smac'] for t in self.transmissions if (t!=transmission) and (t['channel']==transmission['channel'])]
                                                                        
                                    interferenceFlag = 0
                                    for itfr in interferers:

                                        if self.receivers[i]['mote'].getRSSI(itfr)+(-97-(-105))>self.receivers[i]['mote'].minRssi:
					    #print "node "+str(itfr.id)+"is Interferer with "+str(recv['mote'].id)
                                            interferenceFlag = 1
                                            
                                    transmission['smac'].schedule[(ts,transmission['channel'])]['debug_interference'] += [interferenceFlag] # debug only
                                                                                                            
                                    if interferenceFlag:
                                        transmission['smac'].stats_incrementRadioStats('probableCollisions') 
                                    
                                    lockOn = transmission['smac']
                                    for itfr in interferers:
                                        if arrivalTime[itfr] < arrivalTime[lockOn] and self.receivers[i]['mote'].getRSSI(itfr)+(-97-(-105))>self.receivers[i]['mote'].minRssi:
                                            # lock on interference                  
                                            lockOn = itfr
                                    
                                    if lockOn == transmission['smac']:
                                        # mote locked in the current signal
                                        
                                        transmission['smac'].schedule[(ts,transmission['channel'])]['debug_lockInterference'] += [0] # debug only
                                        
                                        # calculate pdr, including interference
                                        sinr  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],interferers,False)
                                        pdr   = self._computePdrFromSINR(sinr, self.receivers[i]['mote'])

					# calculate pdr, without interference to check later if there have been a collision
					sinr2  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],[],False)
					pdr2   = self._computePdrFromSINR(sinr2, self.receivers[i]['mote'])

                                        # pick a random number
                                        failure = random.random() 
   
                                        if pdr>=failure:

                                       	    if self.receivers[i]['mote']==transmission['dmac']:
		                                    isACKed, isNACKed = self.receivers[i]['mote'].radio_rxDone(
		                                        type       = transmission['type'],
		                                        smac       = transmission['smac'],
		                                        dmac       = transmission['dmac'],
		                                        payload    = transmission['payload'],
		                                        channel    = transmission['channel']
		                                    )
						    #Desired 6TOP packet is received, this node can not receive anything else in this cell
		                                    nodesRxAtThisCell.remove(self.receivers[i])

                                            
                                        else: 
					    #here does not mean there is a collision. 
					    #Only means a packet that have a possible interference has failed.
					    if interferenceFlag:
						if pdr2 > 0.0:

							nodesRxAtThisCell.remove(self.receivers[i])
							#collisions in shared cells are not considered for the stats
				    #only for debug
                                    #else:
                                        #mote locked in an interfering signal
#					if transmission['smac'].schedule[(ts,transmission['channel'])]['dir']!='SHARED':
#						transmission['smac'].schedule[(ts,transmission['channel'])]['debug_lockInterference'] += [1]
#						# for debug

#		                                # receive the interference as if it's a desired packet
#		                                interferers.remove(lockOn)
#		                                pseudo_interferers = interferers + [transmission['smac']]
#		                                
#		                                # calculate SINR where locked interference and other signals are considered S and I+N respectively
#		                                pseudo_sinr  = self._computeSINR(lockOn,transmission['dmac'],pseudo_interferers,False)
#		                                pseudo_pdr   = self._computePdrFromSINR(pseudo_sinr, transmission['dmac'])
#		                                
#		                                # pick a random number
#		                                failure = random.random()
#		                                if pseudo_pdr>=failure:
#		                                    # success to receive the interference and realize collision
#		                                    transmission['dmac'].schedule[(ts,transmission['channel'])]['rxDetectedCollision'] = True
                                else:
                                    #================ without interference ========
                                    assert False #only interference model

                        i += 1

		    # desired packet is not received
                    transmission['smac'].radio_txDone(isACKed, isNACKed, transmission['type'])
    
                else:  
		    assert 'DATA' == transmission['type']  

		    self.engine.incrementStatTRX()

                    while i<len(self.receivers):

			if self.receivers[i] in nodesRxAtThisCell:

                            if self.receivers[i]['mote']==transmission['dmac']:
                                                     
                                if not self.settings.noInterference:
      
                                    #================ with interference ===========
                                     
                                    # other transmissions on the same channel?
                                    interferers = [t['smac'] for t in self.transmissions if (t!=transmission) and (t['channel']==transmission['channel'])]
                                   
                                    interferenceFlag = 0
                                    for itfr in interferers:
                                        #if transmission['dmac'].getRSSI(itfr)>transmission['dmac'].minRssi:
                                        if self.receivers[i]['mote'].getRSSI(itfr)+(-97-(-105))>self.receivers[i]['mote'].minRssi:
                                            interferenceFlag = 1

                                    transmission['smac'].schedule[(ts,transmission['channel'])]['debug_interference'] += [interferenceFlag] # debug only
                                                                                                            
                                    if interferenceFlag:
                                        transmission['smac'].stats_incrementRadioStats('probableCollisions') 
                                    
                                    lockOn = transmission['smac']
                                    for itfr in interferers:
                                        if arrivalTime[itfr] < arrivalTime[lockOn] and self.receivers[i]['mote'].getRSSI(itfr)+(-97-(-105))>self.receivers[i]['mote'].minRssi:
                                            # lock on interference
                                            lockOn = itfr
                                    
                                    if lockOn == transmission['smac']:
                                        # mote locked in the current signal
                                        
                                        transmission['smac'].schedule[(ts,transmission['channel'])]['debug_lockInterference'] += [0] # debug only
                                        
                                        # calculate pdr, including interference
                                        sinr  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],interferers,False)
                                        pdr   = self._computePdrFromSINR(sinr, self.receivers[i]['mote'])
					# calculate pdr, without interference to check later if there have been a collision
                                        sinr2  = self._computeSINR(transmission['smac'],self.receivers[i]['mote'],[],False)
                                        pdr2   = self._computePdrFromSINR(sinr2, self.receivers[i]['mote'])

                                        # pick a random number
                                        failure = random.random()

                                        if pdr>=failure:
					
                                       	    if self.receivers[i]['mote']==transmission['dmac']:
						    self.engine.incrementStatRDX()
		                                    isACKed, isNACKed = self.receivers[i]['mote'].radio_rxDone(
		                                        type       = transmission['type'],
		                                        smac       = transmission['smac'],
		                                        dmac       = transmission['dmac'],
		                                        payload    = transmission['payload'],
		                                        channel    = transmission['channel']
		                                    )
						    #Desired DATA packet is received, not expecting more
		                                    nodesRxAtThisCell.remove(self.receivers[i])
					    else:
						assert False	#can not happen

                                        else: 
					    #here does not mean there is a collision. 
					    #Only means a packet that have a possible interference has failed.

                                            if interferenceFlag:
						if pdr2 > 0.0:
							#this is a collision, record it
                                                	self.engine.incrementStatDropByCollision()
							#this mote can not receive anything else
							nodesRxAtThisCell.remove(self.receivers[i])
						else:
							#this is a propagation drop
							self.engine.incrementStatDropByPropagation()
					    else:
						#this is a propagation drop
						self.engine.incrementStatDropByPropagation()                                        
                                    else:
                                        # mote locked in an interfering signal
					#this TX are considered collisions
					self.engine.incrementStatDropByCollision()
                                else:
                                    
                                    #================ without interference ========
                                    assert False #only interference model

                        i += 1

		    # desired packet is not received
                    transmission['smac'].radio_txDone(isACKed, isNACKed, transmission['type'])
            	    
		    #check for each cell that: TX = RX + collision Drops + propagation drops
            	    if self.engine.TRX!=(self.engine.RDX+self.engine.dropByCollision+self.engine.dropByPropagation):
			print "TX "+str(self.engine.TRX)
			print "RX "+str(self.engine.RDX)
			print "Col "+str(self.engine.dropByCollision)
			print "Phy "+str(self.engine.dropByPropagation)
			assert False
	    

            # remaining receivers that does not receive a desired packet
            for r in self.receivers:            
                if not self.settings.noInterference:
		    r['mote'].radio_rxDone(None,None,None,None,r['channel'])
                else: #only model with interference
                    assert False	

            # clear all outstanding transmissions
            self.transmissions              = []
            self.receivers                  = []
        #assert False
        self._schedule_propagate()
    
    #======================== private =========================================
    
    def _schedule_propagate(self):
        with self.dataLock:
            self.engine.scheduleAtAsn(
                asn         = self.engine.getAsn()+1,# so propagation happens in next slot
                cb          = self.propagate,
                uniqueTag   = (None,'propagation'),
                priority    = 1,
            )
    
    def _computeSINR(self,source,destination,interferers,broadcast):
        ''' compute SINR  '''

#        #for Ideal PHY
#        if broadcast==False:
#            for interferer in interferers:   
#                if (destination.getRSSI(interferer)+(-97-(-105)) >= destination.minRssi):
#                    return -10.0

        noise = self._dBmTomW(destination.noisepower)

        # S = RSSI - N
	signal = self._dBmTomW(source.getRSSI(destination))

	#if signal too low, do not waste time
	if source.getRSSI(destination) < destination.noisepower:
            return -10.0

        
        totalInterference = 0.0
        for interferer in interferers:
            # I = RSSI - N
            
	    interference = self._dBmTomW(interferer.getRSSI(destination))

	    if interferer.getRSSI(destination) < destination.noisepower:
                interference = 0.0
            totalInterference += interference

        sinr = signal/(totalInterference + noise)

        return self._mWTodBm(sinr)
    
    def _computePdrFromSINR(self, sinr, destination):
        ''' compute PDR from SINR '''

     
##       #for Ideal PHY    
#        if sinr > 10:
#            return 1
#        else:
#            return 0
        
        equivalentRSSI  = self._mWTodBm(
            self._dBmTomW(sinr+destination.noisepower)##WHY ADDING THE NOISE TWO TIMEs?
        )        

        pdr             = Topology.Topology.rssiToPdr(equivalentRSSI)

        return pdr
    
    def _dBmTomW(self, dBm):
        ''' translate dBm to mW '''
        return math.pow(10.0, dBm/10.0)
    
    def _mWTodBm(self, mW):
        ''' translate dBm to mW '''
        return 10*math.log10(mW)
