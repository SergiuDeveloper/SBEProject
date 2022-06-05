# EBS Project
![plot](./network.png)

## Requirements
https://profs.info.uaic.ro/~eonica/ebs/eval.html#projects

Implementati o arhitectura de sistem publish/subscribe, content-based, structurata in felul urmator:
- Generati un flux de publicatii care sa fie emis de 1-2 noduri publisher. Publicatiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica. (5 puncte)
- Implementati o retea (overlay) de 2-3 brokeri care sa stocheze subscriptii primite de la clienti (subscriberi) si sa-i notifice pe acestia in functie de o filtrare bazata pe continutul publicatiilor. (10 puncte)
- Simulati 2-3 noduri subscriber care se conecteaza aleatoriu la reteaua de brokeri pentru a inregistra susbcriptii. Subscriptiile pot fi generate cu valori aleatoare pentru campuri folosind generatorul de date din tema practica. (5 puncte)
- Implementati un mecanism avansat de rutare la inregistrarea subscriptiilor. Subscriptiile aceluiasi subscriber vor fi distribuite balansat pe mai multi brokeri fiind rutate conform mecanismului implementat. Publicatiile vor trece prin mai multi brokeri pana la destinatie, fiecare ocupandu-se partial de rutarea acestora, si nu doar unul care contine toate subscriptiile si face un simplu match. (5 puncte)
- Realizati o evaluare a sistemului, masurand pentru inregistrarea a 10000 de subscriptii simple, urmatoarele statistici: a) cate publicatii se livreaza cu succes prin reteaua de brokeri intr-un interval continuu de feed de 3 minute, b) latenta medie de livrare a unei publicatii (timpul de la emitere pana la primire) pentru acelasi interval, c) rata de potrivire (matching) pentru cazul in care subscriptiile contin pe unul dintre campuri doar operator de egalitate (100%) comparata cu situatia in care frecventa operatorului de egalitate pe campul respectiv este aproximativ un sfert (25%). Redactati un scurt raport de evaluare a solutiei. (10 puncte)
