#!/bin/bash

echo "==============================="
echo " DEMO HAPROXY - REVERSE PROXY "
echo "==============================="
echo

echo "1) Vérification du statut du service haproxy"
echo "-------------------------------------------"
systemctl status haproxy --no-pager -l | head -n 5
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "2) Vérification de la configuration HAProxy"
echo "-------------------------------------------"
sudo haproxy -c -f /etc/haproxy/haproxy.cfg
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "3) Test HTTP: / → front (vm-front)"
echo "----------------------------------"
echo "Commande : curl http://localhost/"
echo
curl -s http://localhost/ | head -n 5
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "4) Test HTTP: /api/test → API Gateway (vm-gateway)"
echo "---------------------------------------------------"
echo "Commande : curl -v http://localhost/api/test"
echo
curl -v http://localhost/api/test 2>&1 | head -n 15
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "5) Vérifier que HAProxy écoute bien sur les bons ports"
echo "------------------------------------------------------"
sudo ss -tulpn | grep -E ':80|:1883|:8404' || echo "Aucun port trouvé (à vérifier)"
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "6) Test TCP MQTT via le reverse proxy"
echo "-------------------------------------"
echo "Commande : nc -vz localhost 1883"
echo
nc -vz localhost 1883 || echo "Échec de connexion au port 1883 (vérifier Mosquitto ou la conf)"
echo
read -p "Appuie sur Entrée pour continuer..."

echo
echo "7) Test Publish MQTT via HAProxy"
echo "--------------------------------------------"
echo "4) Envoi d'un message MQTT via le reverse proxy"
echo "---------------------------------------"

MESSAGE="HELLO FROM PROXY - $(date)"
mosquitto_pub -h localhost -p 1883 -t test/demo -m "$MESSAGE"

echo ""
echo "Message publié : \"$MESSAGE\""
echo ""
sleep 1
echo
echo "DEMO TERMINEE"