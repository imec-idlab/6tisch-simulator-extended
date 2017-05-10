#!/usr/bin/python
'''
\brief Wireless network topology creator.

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
log = logging.getLogger('Topology')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import random
import math
import numpy as np
import SimSettings
import SimEngine
#============================ defines =========================================

#============================ body ============================================

class Topology(object):
    
    EIGTH_SIX_EIGTH_GHZ         = 868000000   # Hz
    TWO_DOT_FOUR_GHZ         = 2400000000   # Hz
    PISTER_HACK_LOWER_SHIFT  = 40           # -40 dB
    SPEED_OF_LIGHT           = 299792458    # m/s
    
    STABLE_RSSI              =  -89   	     # dBm, corresponds to PDR = 0.8702 (see rssiPdrTable below)
    STABLE_NEIGHBORS         = 1
    
    def __init__(self, motes):
        
        # store params
        self.motes           = motes
        
	#random.seed(13)
	
        # local variables
        self.settings        = SimSettings.SimSettings()
	self.engine        = SimEngine.SimEngine()
        
    #======================== public ==========================================
    
    def createTopology(self):
        '''
        Create a topology in which all nodes have at least STABLE_NEIGHBORS link 
        with enough RSSI.
        If the mote does not have STABLE_NEIGHBORS links with enough RSSI, 
        reset the location of the mote.
        '''
        
        # find DAG root
        dagRoot = None
        for mote in self.motes:
            if mote.id==0:
                mote.role_setDagRoot()
                dagRoot = mote
        assert dagRoot

        if self.settings.mobilityModel!='RPGM':
        	# put DAG root at center of area
		dagRoot.setLocation(
		    x = self.settings.squareSide/2,
		    y = self.settings.squareSide/2

		)
        else:
		#put dag root in the lower left corner
	        dagRoot.setLocation(
	    		x=0.35,
	    		y=2.0
        	)	


	#iniazilizing some variables for the mesh-struct topology
	if self.settings.topology=='mesh-struct': 
		region="null"
		currentLevel=0
		numNodesInThisLevel=0
		alphaPrev=0

		#motes are distributed evenly (aprox) in the different divisions: left up up, left up down, left down up, etc.
		subLevelFilling_luu=0
		subLevelFilling_lud=0
		subLevelFilling_ldu=0
		subLevelFilling_ldd=0
		subLevelFilling_rud=0
		subLevelFilling_rdu=0
		subLevelFilling_rdd=0
		subLevelFilling_ruu=0
		subLevelFilling_ru=0
		subLevelFilling_lu=0
		subLevelFilling_rd=0
		subLevelFilling_ld=0
		
		#set distribution		
		nodesPerLevel=math.ceil((len(self.motes))/(self.settings.maxNumHops))		
		subLevelFilling=math.ceil(len(self.motes)/(self.settings.maxNumHops)/8)

		x=0
		y=0
		
		# increment fo distance at each hop
		alphaDistance=self.settings.squareSide/self.settings.maxNumHops/2
		alphaInitVariance=alphaDistance/25	#add some variability in the placement

		#iteration counter for placing attempts
		it=0

        # reposition each mote until it is connected
        connectedMotes = [dagRoot]
        for mote in self.motes:
            if mote in connectedMotes:
                continue

            connected = False
            while not connected:
           
                numStableNeighbors = 0

		#-------------------------------------------------------#
		#default topology: nodes are added randomly until are sucessfully connected with numStableNeighbors parents
                if self.settings.topology=='mesh':
			# pick a random location
			mote.setLocation(
		            x = self.settings.squareSide*random.random(),
		            y = self.settings.squareSide*random.random()
		        )

		        # count number of neighbors with sufficient RSSI
		        for cm in connectedMotes:
		            
	            	    if self.settings.mobilityModel=='static' or self.settings.mobilityModel=='staticUNI':
			    	rssi = self._computeRSSI_static(mote, cm)
			    else:
				rssi = self._computeRSSI_staticRay(mote, cm)
		            mote.setRSSI(cm, rssi)
		            cm.setRSSI(mote, rssi)
		            
		            if rssi>self.STABLE_RSSI:
		                numStableNeighbors += 1
		                print str(mote.id)+" - "+str(cm.id)+" : "+str(rssi)
                	# make sure it is connected to at least STABLE_NEIGHBORS motes 
                	# or connected to all the currently deployed motes when the number of deployed motes 
                	# are smaller than STABLE_NEIGHBORS
                	if numStableNeighbors >= self.STABLE_NEIGHBORS or numStableNeighbors == len(connectedMotes):
				#avoid the nodes are placed in not valid locations (i.e., obstacles)
				if self.engine.checkValidPositionMotePlacement(mote.x,mote.y,True):
                    			connected = True
		#-------------------------------------------------------#
		# exactly 1 hop network. All nodes connected to the root
                elif self.settings.topology=='star':
			mote.setLocation(
		            x = self.settings.squareSide*random.random(),
		            y = self.settings.squareSide*random.random()
		        )						    

		        if self.settings.mobilityModel=='static' or self.settings.mobilityModel=='staticUNI':
				rssi1 = self._computeRSSI_static(mote, cm)	#use pister hack for initial RSSI 
			else:
				rssi1 = self._computeRSSI_staticRay(mote, cm)	#use rayleigh for initial RSSI 

		        mote.setRSSI(dagRoot, rssi1)
		        dagRoot.setRSSI(mote, rssi1)
		        if rssi1>self.STABLE_RSSI:
		            connected = True
			    for cm in connectedMotes:	
				if cm !=dagRoot:	            
		                    if self.settings.mobilityModel=='static' or self.settings.mobilityModel=='staticUNI':
				    	rssi = self._computeRSSI_static(mote, cm)	#use pister hack for initial RSSI 
				    else:
					rssi = self._computeRSSI_staticRay(mote, cm)	#use rayleigh for initial RSSI 
			            mote.setRSSI(cm, rssi)
		                    cm.setRSSI(mote, rssi)

		#-------------------------------------------------------#
		#motes are placed forcing a specific avg number of hops
                elif self.settings.topology=='mesh-struct': 
		   	    
		    leftOrRight=random.random()
		    upOrDown=random.random()

		    #set the min and max values for x and y in the later placement for the different divisions
		    region=None
		    axisV=None
		    if leftOrRight>=0.5 and upOrDown >=0.5: 				#right down
			if currentLevel==0:
			    if subLevelFilling_rd <= (subLevelFilling*2):
				    region="rd"				
				    xmin=(self.settings.squareSide/2)
				    xmax=(self.settings.squareSide/2)+alphaDistance		
				    ymin=(self.settings.squareSide/2)
				    ymax=(self.settings.squareSide/2)+alphaDistance
			else:
			    supOrInf=random.random()
			    if supOrInf >=0.5:						#right down up
				if subLevelFilling_rdu <= subLevelFilling:
					axisV=True
					region="rdu"
					xmin=(self.settings.squareSide/2)+alphaPrev
				    	xmax=(self.settings.squareSide/2)+alphaDistance		
				    	ymin=(self.settings.squareSide/2)
				    	ymax=(self.settings.squareSide/2)+alphaPrev
			    else:							#right down down
				if subLevelFilling_rdd <= subLevelFilling:
					axisV=False
					region="rdd"
					xmin=(self.settings.squareSide/2)
				    	xmax=(self.settings.squareSide/2)+alphaDistance		
				    	ymin=(self.settings.squareSide/2)+alphaPrev
				    	ymax=(self.settings.squareSide/2)+alphaDistance
		    if leftOrRight>=0.5 and upOrDown <0.5:  				#right up
			if currentLevel==0:
			    if subLevelFilling_ru <= (subLevelFilling*2):
				    region="ru"				
				    xmin=(self.settings.squareSide/2)
				    xmax=(self.settings.squareSide/2)+alphaDistance		
				    ymin=(self.settings.squareSide/2)-alphaDistance
				    ymax=(self.settings.squareSide/2)
			else:
			    supOrInf=random.random()
			    if supOrInf >=0.5:						#right up up
				if subLevelFilling_ruu <= subLevelFilling:
					axisV=False
					region="ruu"
					xmin=(self.settings.squareSide/2)
				    	xmax=(self.settings.squareSide/2)+alphaDistance		
				    	ymin=(self.settings.squareSide/2)-alphaDistance	
				    	ymax=(self.settings.squareSide/2)-alphaPrev
			    else:							#right up down
				if subLevelFilling_rud <= subLevelFilling:
					axisV=True
					region="rud"
					xmin=(self.settings.squareSide/2)+alphaPrev
				    	xmax=(self.settings.squareSide/2)+alphaDistance		
				    	ymin=(self.settings.squareSide/2)-alphaPrev
				    	ymax=(self.settings.squareSide/2)
		    if leftOrRight<0.5 and upOrDown >=0.5:   				 #left down			
			if currentLevel==0:
			    if subLevelFilling_ld <= (subLevelFilling*2):
				    region="ld"				
				    xmin=(self.settings.squareSide/2)-alphaDistance
				    xmax=(self.settings.squareSide/2)		
				    ymin=(self.settings.squareSide/2)
				    ymax=(self.settings.squareSide/2)+alphaDistance
			else:
			    supOrInf=random.random()
			    if supOrInf >=0.5:						#left down up
				if subLevelFilling_ldu <= subLevelFilling:
					region="ldu"
					axisV=True
					xmin=(self.settings.squareSide/2)-alphaDistance	
				    	xmax=(self.settings.squareSide/2)-alphaPrev	
				    	ymin=(self.settings.squareSide/2)
				    	ymax=(self.settings.squareSide/2)+alphaPrev
			    else:							#left down down
				if subLevelFilling_ldd <= subLevelFilling:
					region="ldd"
					axisV=False	
					xmin=(self.settings.squareSide/2)-alphaDistance	
				    	xmax=(self.settings.squareSide/2)	
				    	ymin=(self.settings.squareSide/2)+alphaPrev
				    	ymax=(self.settings.squareSide/2)+alphaDistance
		    if leftOrRight<0.5 and upOrDown <0.5:				 #left up			
			if currentLevel==0:	
			    if subLevelFilling_lu <= (subLevelFilling*2):
				    region="lu"				
				    xmin=(self.settings.squareSide/2)-alphaDistance
				    xmax=(self.settings.squareSide/2)		
				    ymin=(self.settings.squareSide/2)-alphaDistance
				    ymax=(self.settings.squareSide/2)			    
			else:								
			    supOrInf=random.random()
			    if supOrInf >=0.5:						#left up up
				if subLevelFilling_luu <= subLevelFilling:
					region="luu"
					axisV=False
					xmin=(self.settings.squareSide/2)-alphaDistance	
				    	xmax=(self.settings.squareSide/2)	
				    	ymin=(self.settings.squareSide/2)-alphaDistance
				    	ymax=(self.settings.squareSide/2)-alphaPrev
			    else:							#left up down
				if subLevelFilling_lud <= subLevelFilling:
					region="lud"
					axisV=True
					xmin=(self.settings.squareSide/2)-alphaDistance	
				    	xmax=(self.settings.squareSide/2)-alphaPrev						
				    	ymin=(self.settings.squareSide/2)-alphaPrev
				    	ymax=(self.settings.squareSide/2)

		    if (subLevelFilling_lu+subLevelFilling_ld+subLevelFilling_ru+subLevelFilling_rd)>=(subLevelFilling*4):
			    subLevelFilling_lu-=1
			    subLevelFilling_ld-=1
			    subLevelFilling_ru-=1
			    subLevelFilling_rd-=1

		    if region!=None:
			    if axisV==None:	#current level 0
				mote.setLocation(
				    x = random.uniform(xmin,xmax),		
				    y = random.gauss(((ymax+ymin)/2),(alphaInitVariance))			
				)
			
			    else:		# level > 0
				#placing mote vertically uniform, horizontally gaussian
				if axisV==True:		
				    mote.setLocation(			
					x=random.gauss(((xmax+xmin)/2),(alphaInitVariance)),
					y=random.uniform(ymin,ymax)
			    	    )
				#placing mote horizontally uniform, vertically gaussian
				else:
				    mote.setLocation(			
					x=random.uniform(xmin,xmax),
					y=random.gauss(((ymax+ymin)/2),(alphaInitVariance))
			    	    )

			    for cm in connectedMotes:
				    if self.settings.mobilityModel=='static' or self.settings.mobilityModel=='staticUNI':
					rssi = self._computeRSSI_static(mote, cm)	#use pister hack for initial RSSI 
				    else:
					rssi = self._computeRSSI_staticRay(mote, cm)	#use rayleigh for initial RSSI 

				    mote.setRSSI(cm, rssi)
				    cm.setRSSI(mote, rssi)
				    
				    if rssi>self.STABLE_RSSI:
					connected = True
					it=0	#reset attempt counter
					continue
			    #increase counters in division and level
			    if connected==True:
								
				numNodesInThisLevel+=1	
				if region=="luu":
					subLevelFilling_luu+=1				
				if region=="lud":
					subLevelFilling_lud+=1				
				if region=="ldu":
					subLevelFilling_ldu+=1
				if region=="ldd":
					subLevelFilling_ldd+=1
				if region=="rud":
					subLevelFilling_rud+=1
				if region=="rdu":
					subLevelFilling_rdu+=1
				if region=="rdd":
					subLevelFilling_rdd+=1
				if region=="ruu":
					subLevelFilling_ruu+=1	
				if region=="lu":
					subLevelFilling_lu+=1	
				if region=="ru":
					subLevelFilling_ru+=1
				if region=="ld":
					subLevelFilling_ld+=1
				if region=="rd":
					subLevelFilling_rd+=1
			
				#this level has been already filled.
				if numNodesInThisLevel >= nodesPerLevel:	
	
				    #going to the next level				    
				    if currentLevel < (self.settings.maxNumHops-1):  
					numNodesInThisLevel=0        
				    	currentLevel+=1
					alphaPrev=alphaDistance
				    	alphaDistance+=(self.settings.squareSide/self.settings.maxNumHops/2)
				    	subLevelFilling_luu=0
					subLevelFilling_lud=0
					subLevelFilling_ldu=0
					subLevelFilling_ldd=0
					subLevelFilling_rud=0
					subLevelFilling_rdu=0
					subLevelFilling_rdd=0
					subLevelFilling_ruu=0
					subLevelFilling_rd=0
					subLevelFilling_ld=0
					subLevelFilling_ru=0
					subLevelFilling_lu=0
					
				    #this is the last level: place the remaining mote in this level
				    else:					
					numNodesInThisLevel=0
				    	subLevelFilling_luu=0
					subLevelFilling_lud=0
					subLevelFilling_ldu=0
					subLevelFilling_ldd=0
					subLevelFilling_rud=0
					subLevelFilling_rdu=0
					subLevelFilling_rdd=0
					subLevelFilling_ruu=0
					subLevelFilling_rd=0
					subLevelFilling_ld=0
					subLevelFilling_ru=0
					subLevelFilling_lu=0

			    # not successfully connected. 
			    if connected==False:
				it+=1				
				#if impossible to place, reset the division conters and place it wherever is possible.
				if it>1000:					
					subLevelFilling_rd=0
					subLevelFilling_ld=0
					subLevelFilling_ru=0
					subLevelFilling_lu=0 
					subLevelFilling_luu=0
					subLevelFilling_lud=0
					subLevelFilling_ldu=0
					subLevelFilling_ldd=0
					subLevelFilling_rud=0
					subLevelFilling_rdu=0
					subLevelFilling_rdd=0
					subLevelFilling_ruu=0
	    #one mote has been connected 
            connectedMotes += [mote]
		
	# for each mote, compute PDR to each neighbors
	for mote in self.motes:
	    for m in self.motes:
	        if mote==m:
	            continue
	        if mote.getRSSI(m)>mote.minRssi:
	            pdr = self._computePDR(mote,m)
	            mote.setPDR(m,pdr)
	            m.setPDR(mote,pdr)       
        
    #@profile	
    def updateTopology(self):
        '''
        update topology: re-calculate RSSI values. For scenarios != static
        '''

	for mote1 in self.motes:
		for mote2 in self.motes:
		    if mote1.id != mote2.id:
			rssi = self._computeRSSI_mobility(mote1, mote2)		#the RSSI is calculated with applying a random variation
			mote1.setRSSI(mote2, rssi)
		        mote2.setRSSI(mote1, rssi)
			if rssi>mote1.minRssi:
			    	pdr = self.rssiToPdr(rssi)
			    	mote1.setPDR(mote2,pdr)
			   	mote2.setPDR(mote1,pdr)
			else:
			    	pdr = 0
			    	mote1.setPDR(mote2,pdr)
			   	mote2.setPDR(mote1,pdr)
   
    #======================== private =========================================

    def _computeRSSI_mobility(self,mote,neighbor):
        ''' computes RSSI between any two nodes (not only neighbors) for mobility scenarios applying a 12dB uniform variation'''
       
	mu=mote.staticPhys[neighbor]
	rssi=random.uniform(-6, 6)+mu
	return rssi

    def _computeRSSI_staticRay(self,mote,neighbor):
        ''' computes RSSI between any two nodes (not only neighbors) according to the rayleigh model for the first time.'''

	# distance in m
	distance = self._computeDistance(mote,neighbor)
	
	# sqrt and inverse of the free space path loss
	fspl = (self.SPEED_OF_LIGHT/(4*math.pi*distance*self.TWO_DOT_FOUR_GHZ))
	
	# simple friis equation in Pr=Pt+Gt+Gr+20log10(c/4piR)
	pr = mote.txPower + mote.antennaGain + neighbor.antennaGain + (20*math.log10(fspl))
	
	#using Rayleighmodel instead of pister-hack
	meanvalue = math.pow(10.0,pr/10.0)
	modevalue = np.sqrt(2 / np.pi) * meanvalue
	rssi = np.random.rayleigh(modevalue, 1)

	#save the first rssi value calculated for future use
	mote.staticPhys[neighbor]=10*math.log10(rssi)
	neighbor.staticPhys[mote]=10*math.log10(rssi)

	return 10*math.log10(rssi)
    

    def _computeRSSI_static(self,mote,neighbor):
        ''' computes RSSI between any two nodes (not only neighbors) according to the Pister-hack model for the first time.'''

	# distance in m
	distance = self._computeDistance(mote,neighbor)
	
	# sqrt and inverse of the free space path loss
	fspl = (self.SPEED_OF_LIGHT/(4*math.pi*distance*self.TWO_DOT_FOUR_GHZ))
	
	# simple friis equation in Pr=Pt+Gt+Gr+20log10(c/4piR)
	pr = mote.txPower + mote.antennaGain + neighbor.antennaGain + (20*math.log10(fspl))

	# according to the receiver power (RSSI) we can apply the Pister hack model.	 
	mu = pr-self.PISTER_HACK_LOWER_SHIFT/2 #chosing the "mean" value

	# the receiver will receive the packet with an rssi uniformly distributed between friis and friis -40
	rssi = mu + random.uniform(-self.PISTER_HACK_LOWER_SHIFT/2, self.PISTER_HACK_LOWER_SHIFT/2)

	#save the first rssi value calculated for future use
	mote.staticPhys[neighbor]=rssi
	neighbor.staticPhys[mote]=rssi

	return rssi


    def _computePDR(self,mote,neighbor):
        ''' computes pdr to neighbor according to RSSI'''
        
        rssi        = mote.getRSSI(neighbor)
        return self.rssiToPdr(rssi)
    
    @classmethod
    def rssiToPdr(self,rssi):
        '''
        rssi and pdr relationship obtained by experiment below
        http://wsn.eecs.berkeley.edu/connectivity/?dataset=dust
        '''
        
	#TODO set rssiPdrTable for the 868 MHz band -> FSK 500KHz, 500kbps 

        rssiPdrTable    = {
            -97:    0.0000, # this value is not from experiment
            -96:    0.1494,
            -95:    0.2340,
            -94:    0.4071,
            #<-- 50% PDR is here, at RSSI=-93.6
            -93:    0.6359,
            -92:    0.6866,
            -91:    0.7476,
            -90:    0.8603,
            -89:    0.8702,
            -88:    0.9324,
            -87:    0.9427,
            -86:    0.9562,
            -85:    0.9611,
            -84:    0.9739,
            -83:    0.9745,
            -82:    0.9844,
            -81:    0.9854,
            -80:    0.9903,
            -79:    1.0000, # this value is not from experiment
        }
        
        minRssi         = min(rssiPdrTable.keys())
        maxRssi         = max(rssiPdrTable.keys())

        if   rssi<minRssi:
            pdr         = 0.0
        elif rssi>maxRssi:
            pdr         = 1.0
        else:
            floorRssi   = int(math.floor(rssi))
            pdrLow      = rssiPdrTable[floorRssi]
            pdrHigh     = rssiPdrTable[floorRssi+1]
            pdr         = (pdrHigh-pdrLow)*(rssi-float(floorRssi))+pdrLow # linear interpolation
        
        assert pdr>=0.0
        assert pdr<=1.0
          
        return pdr
    
    def _computeDistance(self,mote,neighbor):
        '''
        mote.x and mote.y are in km. This function returns the distance in m.
        '''
        
        return 1000*math.sqrt(
            (mote.x - neighbor.x)**2 +
            (mote.y - neighbor.y)**2
        )

#============================ main ============================================
        
def main():
    import Mote
    import SimSettings
    
    NOTVISITED     = 'notVisited'
    MARKED         = 'marked'
    VISITED        = 'visited'
    
    allRanks = []
    for _ in range(100):
        print '.',
        # create topology
        settings                           = SimSettings.SimSettings()
        settings.numMotes                  = 50
        settings.pkPeriod                  = 1.0
        settings.otfHousekeepingPeriod     = 1.0
        settings.sixtopPdrThreshold        = None
        settings.sixtopHousekeepingPeriod  = 1.0
        settings.minRssi                   = None
        settings.squareSide                = 2.0
        settings.slotframeLength           = 101
        settings.slotDuration              = 0.010
        settings.sixtopNoHousekeeping      = 0
        settings.numPacketsBurst           = None
        motes                              = [Mote.Mote(id) for id in range(settings.numMotes)]
        topology                           = Topology(motes)
        topology.createTopology()
        
        # print stats
        hopVal    = {}
        moteState = {}
        for mote in motes:
            if mote.id==0:
                hopVal[mote]     = 0
                moteState[mote]  = MARKED
            else:
                hopVal[mote]     = None
                moteState[mote]  = NOTVISITED
        
        while (NOTVISITED in moteState.values()) or (MARKED in moteState.values()):
            
            # find marked mote
            for (currentMote,s) in moteState.items():
                if s==MARKED:
                   break
            assert moteState[currentMote]==MARKED
            
            # mark all of its neighbors with pdr >50%
            for neighbor in motes:
                try:
                    if currentMote.getPDR(neighbor)>0.5:
                        if moteState[neighbor]==NOTVISITED:
                            moteState[neighbor]      = MARKED
                            hopVal[neighbor]         = hopVal[currentMote]+1
                        if moteState[neighbor]==VISITED:
                            if hopVal[currentMote]+1<hopVal[neighbor]:
                                hopVal[neighbor]     = hopVal[currentMote]+1
                except KeyError as err:
                    pass # happens when no a neighbor
            
            # mark it as visited
            moteState[currentMote]=VISITED
        
        allRanks += hopVal.values()
    
    assert len(allRanks)==100*50
    
    print ''
    print 'average rank: {0}'.format(float(sum(allRanks))/float(len(allRanks)))
    print 'max rank:     {0}'.format(max(allRanks))
    print ''
    
    raw_input("Script ended. Press Enter to close.")
    
if __name__=="__main__":
    main()
