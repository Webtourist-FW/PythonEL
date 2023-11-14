# Neue Ziele definieren

Jedes neue Ziel sollte:

- mind. von der Target-Klasse erben, evtl. sogar von einer spezielleren Klasse, z.B. FileTarget
- eine ´__init__´-Methode definieren, die super() aufruft und die Parameter übergibt (macht die IDE idealerweise automatisch)
- eine load-Methode definieren,
  - welche die Lade-Logik handhabt und von den Jobs aufgerufen werden kann
  - mit einer Liste / einem Generator von DataPackages umgehen können

Jedes neue Ziel muss im Parser (parser.py) eingetragen werden, damit es gefunden wird.

Jede Logik oder Parameter, die von mehreren Zielen genutzt wird, sollte auf eine darüber liegende Ebene gehoben werden.