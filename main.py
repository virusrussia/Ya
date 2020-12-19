'''
Created on 17 дек. 2020 г.

@author: alex
'''

from YaModule import YaModule

q = YaModule('55854289', 'AgAAAAAJaXxbAAbExAcdn0XYjkcAgVC0STsJ7I0')

q.YaSetDates("01.01.2020", "18.12.2020")

#print(q.YaCreateQuery(q.df.loc["visits"][(q.df.loc["visits"].index==0) | (q.df.loc["visits"].index > 90)], "visits"))

ch = q.YaCheckQuerys()

print (ch)

print (q.YaPossibleQuery(q.df.loc["hits"], "hits"))

#df = q.YaDownloadQuery(input("Номер запроса"), "visits")
#df.to_excel("visits1.xlsx")