# Reverse proxy

**Reverse proxy utilisé :** HA Proxy

## VM :

**DNS VM :** vm-reverse-proxy

**Adresse IP :** 172.31.253.58

**Mot de passe :** toto


### <ins>Emplacement VM</ins> :

VSpère :

```
EPISE-Creteil
└── Etudiants
    ├── SIRIUS
        ├── Promo 2023-2026
            ├── Groupe 1 - EPIGREEN-SHOP
                ├── ING3
                    ├── Front
                        └── vm-reverse-proxy
```


#### Configuration :
/etc/haproxy/haproxy.cfg

### Architecture Réseau

Le reverse proxy (HAProxy) redirige les flux vers les services internes suivants :

| Service | IP Privée (Backend) | Port | Protocole | Rôle |
| :--- | :--- | :--- | :--- | :--- |
| **Frontend Web** | `172.31.252.12` | 80 | HTTP | Interface utilisateur|
| **API Backend** | `172.31.253.166` | 8080 | HTTP | Logique métier (Route `/api/`) |
| **MQTT Broker** | `172.31.252.234` | 1883 | TCP | Bus de messages IoT |
| **PostgreSQL** | `172.31.250.155` | 5432 | TCP | Base de données relationnelle |