# Multi-Worker, Phase 4: Linux Print-&-Alarm-Gateway

Basis: main `fdfb4a2`. Es wird kein produktives Gateway im Rahmen dieser Änderung aktualisiert.
Die zugehörige Cloud benötigt Phase 2/3 sowie Migration 0314.

## Druckgrenzen

Die Cloud sendet eine stabile Job-ID. SQLite nimmt denselben Job nur einmal auf.
Vor jedem CUPS-Aufruf wird der lokale Intent atomar committet. Konkurrierende
Spool-Durchläufe, erneut eintreffende Nachrichten und spätere Reconnects dürfen
keinen zweiten CUPS-Aufruf auslösen. Nach Cleanup bleiben IDs als Dedup-Tombstones
erhalten; für einen gezielten Nachdruck muss die Cloud eine neue ID anlegen.

Das Gateway meldet `print-status-v2`. Enthält der Auftrag `requires_intent`, werden
Token und Generation gespeichert. Nach Download/Rendering fordert es die Cloud per
`print_intent` zur Freigabe auf. Erst eine passende positive RPC-Antwort erlaubt
den lokalen Intent und die CUPS-Übergabe. Der WS-Reader bleibt während des Druckens
frei und kann die Freigabe beantworten. Ein unklarer oder abgeschlossener Cloud-
Auftrag erhält keine neue Freigabe. Eine doppelte Nachricht kann die gespeicherte
Intentpflicht nicht entfernen. Für abgelaufene signierte Artefakt-URLs existiert
`refresh_artifact`; nur ein lokal noch ungesendeter Job übernimmt die neue URL.

Ein Neustart mit lokalem Übergabe-Intent ohne CUPS-ID ergibt `uncertain`. Vorhandene
CUPS-IDs werden nur gepollt. CUPS-Abbruch, Fehler nach Übergabe und Abbruch eines
laufenden Jobs gelten als möglicherweise bereits ausgegeben; es gibt keinen
automatischen neuen Druck. Download-/Konfigurationsfehler vor Intent behalten den
begrenzten Retry. Ein fehlendes CUPS-Backend erzeugt keinen Fake-Erfolg; die anderen
Gateway-Funktionen laufen weiter, Druck bleibt ausstehend.

`job_status_ack` bestätigt die DB-Verarbeitung. Bis zum passenden ACK bleiben
Statusmeldungen in der lokalen Outbox und werden auch nach Reconnect wiederholt.
Ein ACK für einen älteren Status quittiert nicht den jüngeren Zustand. Bis die
Cloud-Fähigkeiten ausgehandelt sind, wird ein bloßer Socket-Send nicht als
Bestätigung behandelt. An einer alten Cloud bleiben das alte `print_job` und die
bisherige Transportbestätigung kompatibel; dort fehlen die neuen Cloud-Nachweise.

## Tests und Betriebsgrenzen

49 Python-Tests bestehen. Sie decken unter anderem CUPS-Verzögerung, lokalen
Neustart, parallele Manager, unklare Übergabe, Tombstones, RPC-Zuordnung,
Intentverweigerung, alte ACKs und die Kompatibilitätsaushandlung ab.
Die Cloud testet separat Workergrenzen, echte MariaDB-Konkurrenz und Redis-Neustarts.
Diese Tests verwenden Fake-CUPS-Backends. Ein physischer Linux-Ausdruck ist ohne
freigegebenen Testdrucker noch offen und bleibt Voraussetzung der Geräteabnahme.

Zum Rollback zuerst neue Jobs stoppen und Cloud-Aufträge sowie lokalen Spool
abgleichen und sichern. `submitting`, `printing` und `uncertain` nicht auf `pending`
zurücksetzen. Ein altes Binary enthält wieder die früheren lokalen Retry-Risiken;
deshalb erst nach Drain beziehungsweise manueller Prüfung zurückwechseln. Die
SQLite-Datei nicht als vermeintliche Reparatur löschen.
