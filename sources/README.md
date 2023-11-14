# Neue Quellen definieren

Jede neue Quelle sollte:

- mind. von der Source-Klasse erben, evtl. sogar von einer spezielleren Klasse, z.B. FileSource oder SQLSource
- eine ´__init__´-Methode definieren, die super() aufruft und die Parameter übergibt (macht die IDE idealerweise automatisch)
- eine extract-Methode definieren,
  - welche die Extraktions-Logik handhabt und von den Jobs aufgerufen werden kann
  - welche idealerweise einen Generator für DataPackages, zumindest jedoch eine Liste mit den DataPackages zurückgibt
- DataPackages als Austausch-Format benutzen

Jede neue Quelle muss im Parser (parser.py) eingetragen werden, damit sie gefunden wird.

Jede Logik oder Parameter, die von mehreren Quellen genutzt wird, sollte auf eine darüber liegende Ebene gehoben werden.