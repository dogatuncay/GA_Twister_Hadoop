
import random
from xml.dom.minidom import parseString


file=open('/home/med/Desktop/bioInfo.xml', 'r')
data= file.read()
dom=parseString(data)

f = open('/home/med/Desktop/seedpopulation.txt', "w")

PS=dom.getElementsByTagName('problemSize')[0].toxml()
PopS=dom.getElementsByTagName('populationSize')[0].toxml()

ProblemSize =PS.replace('<problemSize>','').replace('</problemSize>','')
PopulationSize=PopS.replace('<populationSize>','').replace('</populationSize>','')

psint=int(ProblemSize)+1
popsint=int(PopulationSize)+1

for i in range(1,popsint):
    for l in range(1, psint):
        x=random.randrange(0,2)
        y=str(x)
        f.write(y)
    f.write('\n')
