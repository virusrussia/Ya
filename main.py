'''
Created on 17 дек. 2020 г.

@author: alex
'''

from YaModule import YaModule

q = YaModule('55854289', 'AgAAAAAJaXxbAAbExAcdn0XYjkcAgVC0STsJ7I0')

q.YaSetDates("01.01.2020", "20.12.2020")

# print(q.YaCreateQuery(q.df.loc["visits"][(q.df.loc["visits"].index==0) | (q.df.loc["visits"].index > 90)], "visits"))
ch = q.YaCheckQuerys()

# print(q.YaCreateQuery(q.df.loc["visits"][:90], "visits"))

ch = q.YaCheckQuerys()

print (ch)

df = q.YaDownloadQuery(input("Номер запроса"))
df.to_excel("visits1.xlsx")
