# Guía de despliegue — Panel de Ventas Gaven (Hetzner)

Esta guía te lleva de cero a tener el panel online, con tu dominio y HTTPS.
Tu proyecto tiene dos partes:

- **app.py** → el panel de Streamlit (lo que ve la gente en el navegador).
- **data_pipeline.py** → trae las ventas del API de Chess y guarda `data/ventas_actualizadas.parquet`. Hay que correrlo 2 veces por día.

La idea final es:

```
Internet → tu dominio (HTTPS) → Nginx → Streamlit (app.py) → lee el parquet
                                          ↑
                              cron corre data_pipeline.py 2 veces/día
```

> Antes de empezar, tené a mano: el nombre de tu dominio, y las credenciales del
> API de Chess (las que están hoy en tu `.streamlit/secrets.toml` local).

---

## Parte 1 — Crear el servidor en Hetzner

En la consola de Hetzner (la pantalla donde decía "No project has been created yet"):

1. Clic en **+ New Project** → ponele un nombre, ej. `Gaven`.
2. Entrá al proyecto → **Add Server** (o "Create Server").
3. Elegí estas opciones:
   - **Location**: la más cercana (ej. Falkenstein o Nuremberg en Europa; no hay datacenter en Argentina, cualquiera anda bien).
   - **Image**: **Ubuntu 24.04**.
   - **Type**: **CX22** (2 vCPU, 4 GB RAM) — sobra para este panel. Es el más barato compartido, ~4 €/mes.
   - **Networking**: dejá IPv4 activado.
   - **SSH keys**: si no tenés una clave SSH, más abajo (Parte 3) te explico crear una. Por ahora podés elegir **"Set root password"** y anotar la contraseña que te muestra. *(Más adelante conviene usar clave SSH, pero para arrancar la contraseña sirve.)*
   - **Name**: `gaven-panel`.
4. Clic en **Create & Buy now**.

Cuando termine, Hetzner te muestra la **IP pública** del servidor (algo como `5.75.xxx.xxx`). **Anotala**, la vas a usar todo el tiempo.

---

## Parte 2 — Apuntar tu dominio al servidor

Andá al panel donde administrás tu dominio (donde lo compraste: GoDaddy, Namecheap, Cloudflare, etc.) y creá un registro **A**:

| Tipo | Nombre              | Valor (apunta a)        |
|------|---------------------|-------------------------|
| A    | `panel` (o `@`)     | la IP de tu servidor    |

- Si querés `panel.tudominio.com` → Nombre = `panel`.
- Si querés el dominio raíz `tudominio.com` → Nombre = `@`.

Guardá. El DNS puede tardar de unos minutos a un par de horas en propagarse.
Podés verificar con: en tu compu, abrí una terminal y escribí `ping panel.tudominio.com` — tiene que responder con la IP de tu servidor.

> En el resto de la guía voy a usar `panel.tudominio.com` como ejemplo. Reemplazalo por el tuyo.

---

## Parte 3 — Conectarte al servidor por SSH

SSH es la forma de "entrar" al servidor desde tu compu, por terminal.

**En Windows** abrí **PowerShell** (botón inicio → escribí "PowerShell").

Conectate (reemplazá por tu IP):

```bash
ssh root@5.75.xxx.xxx
```

- La primera vez te pregunta si confiás en el host → escribí `yes`.
- Si elegiste contraseña en Hetzner, pegala (no se ve mientras escribís, es normal).

Si ves algo como `root@gaven-panel:~#`, ya estás adentro. 🎉

> **(Opcional pero recomendado) Crear una clave SSH** para no usar contraseña:
> en tu PowerShell local corré `ssh-keygen` (Enter a todo). Luego subí la clave con
> `ssh-copy-id root@TU_IP` (o pegá el contenido de `~/.ssh/id_rsa.pub` en la sección
> SSH Keys de Hetzner). No es obligatorio para que esto funcione.

---

## Parte 4 — Preparar el servidor (instalar lo necesario)

Ya conectado por SSH, copiá y pegá estos comandos (uno a uno o todos juntos):

```bash
apt update && apt upgrade -y
apt install -y python3-venv python3-pip git nginx
```

Esto instala Python, Git y Nginx (el servidor web que va a poner tu panel detrás de tu dominio).

---

## Parte 5 — Traer tu código desde GitHub

Tu repo es privado (`JuanCruzSoria-Dinamica/Gaven`), así que el servidor necesita permiso para leerlo. Lo más limpio es una **clave de despliegue (deploy key)** de solo lectura.

**5.1 — Generá una clave SSH en el servidor:**

```bash
ssh-keygen -t ed25519 -C "gaven-server" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
```

Copiá TODO lo que imprime el último comando (empieza con `ssh-ed25519 ...`).

**5.2 — Pegá esa clave en GitHub:**

1. En el navegador: andá a tu repo → **Settings** → **Deploy keys** → **Add deploy key**.
2. Title: `servidor hetzner`. Key: pegá lo que copiaste. Dejá "Allow write access" **desmarcado**.
3. **Add key**.

**5.3 — Cloná el repo en el servidor:**

```bash
cd /root
git clone git@github.com:JuanCruzSoria-Dinamica/Gaven.git
cd Gaven
```

(Si pregunta por confianza del host de github, escribí `yes`.)
Ahora tu código está en `/root/Gaven`.

---

## Parte 6 — Entorno de Python, credenciales y primer dato

**6.1 — Crear el entorno virtual e instalar dependencias:**

```bash
cd /root/Gaven
python3 -m venv env
env/bin/pip install --upgrade pip
env/bin/pip install -r requirements.txt
```

**6.2 — Crear el archivo de credenciales** (no está en GitHub porque es secreto, así que lo creás a mano en el servidor):

```bash
nano .streamlit/secrets.toml
```

Pegá adentro tus credenciales reales del API de Chess, con este formato:

```toml
[chess]
base_url = "https://EL_QUE_USES"
usuario  = "TU_USUARIO"
password = "TU_PASSWORD"
```

> Mirá tu `secrets.toml` local (en tu PC) para copiar los valores exactos.

Guardá en nano con **Ctrl+O** → Enter, y salí con **Ctrl+X**.

**6.3 — Generar los datos por primera vez:**

```bash
env/bin/python data_pipeline.py
```

Si todo va bien, crea `data/ventas_actualizadas.parquet`. Si da error de credenciales, revisá el `secrets.toml`.

---

## Parte 7 — Dejar Streamlit corriendo siempre (servicio systemd)

Si corrés `streamlit run app.py` a mano, se apaga cuando cerrás la terminal. Para que quede prendido 24/7 y se reinicie solo si el servidor se reinicia, lo registramos como un servicio.

```bash
nano /etc/systemd/system/gaven.service
```

Pegá esto tal cual:

```ini
[Unit]
Description=Panel Gaven (Streamlit)
After=network.target

[Service]
WorkingDirectory=/root/Gaven
ExecStart=/root/Gaven/env/bin/streamlit run app.py --server.port 8501 --server.address 127.0.0.1 --server.headless true
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

Guardá (Ctrl+O, Enter, Ctrl+X) y activalo:

```bash
systemctl daemon-reload
systemctl enable --now gaven
systemctl status gaven
```

Tiene que decir **active (running)**. (Salí del status con `q`.)
En este punto el panel ya corre internamente en el puerto 8501, pero todavía no es accesible desde tu dominio. Eso es la siguiente parte.

---

## Parte 8 — Nginx: conectar tu dominio al panel

```bash
nano /etc/nginx/sites-available/gaven
```

Pegá esto (cambiá `panel.tudominio.com` por el tuyo):

```nginx
server {
    listen 80;
    server_name panel.tudominio.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        # necesario para Streamlit (websockets):
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
    }
}
```

Guardá y activá el sitio:

```bash
ln -s /etc/nginx/sites-available/gaven /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t        # tiene que decir "syntax is ok" y "test is successful"
systemctl restart nginx
```

Ahora abrí en el navegador `http://panel.tudominio.com` → deberías ver el panel.
(Si no carga, esperá a que el DNS de la Parte 2 termine de propagarse.)

---

## Parte 9 — HTTPS (candadito verde, gratis)

```bash
apt install -y certbot python3-certbot-nginx
certbot --nginx -d panel.tudominio.com
```

- Te pide un email (para avisos de renovación) → poné el tuyo.
- Aceptá los términos.
- Cuando pregunte por redirección, elegí la opción **2 (redirigir HTTP a HTTPS)**.

Certbot configura el certificado solo y lo **renueva automáticamente**.
Listo: `https://panel.tudominio.com` ya funciona con candadito. ✅

---

## Parte 10 — Programar el pipeline 2 veces por día (cron)

El panel solo lee el parquet; alguien tiene que actualizarlo. Lo automatizamos con cron a las 08:00 y 20:00.

```bash
crontab -e
```

(Si pregunta el editor, elegí `nano` → opción 1.) Pegá al final esta línea:

```
0 8,20 * * * cd /root/Gaven && /root/Gaven/env/bin/python data_pipeline.py >> /root/Gaven/pipeline.log 2>&1
```

Guardá (Ctrl+O, Enter, Ctrl+X).
El `cd /root/Gaven` es importante: así el pipeline encuentra el `.streamlit/secrets.toml`.
Los errores/registros quedan en `/root/Gaven/pipeline.log` (lo ves con `cat /root/Gaven/pipeline.log`).

---

## Parte 11 — Firewall (seguridad básica)

```bash
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable
```

Esto deja pasar solo SSH y web (80/443) y bloquea el resto.

> **Nota sobre el acceso:** elegiste dejar el panel abierto (sin login). Como muestra
> ventas, cualquiera con el link puede verlo. Si más adelante querés ponerle usuario y
> contraseña, se puede agregar fácil con Nginx (basic auth) — pedímelo y te paso los pasos.

---

## ✅ Listo

Tu panel está online en `https://panel.tudominio.com`, corriendo solo, con datos que se actualizan 2 veces por día.

---

# Tu otra pregunta: cuando cambio el código, ¿se actualiza solo?

**No, no se actualiza solo.** El servidor tiene una *copia* de tu código (la que clonaste). Cuando vos cambiás algo en tu PC y lo subís a GitHub, el servidor **no se entera** hasta que le decís que baje los cambios.

El flujo cada vez que hagas un cambio es:

**1) En tu PC** — subís el cambio a GitHub como siempre:

```bash
git add .
git commit -m "lo que cambiaste"
git push
```

**2) En el servidor** — entrás por SSH y bajás los cambios:

```bash
ssh root@TU_IP
cd /root/Gaven
git pull
systemctl restart gaven
```

Ese `git pull` trae el código nuevo y `restart gaven` reinicia el panel para que tome los cambios.

### Atajo: script de actualización

Para no escribir todo cada vez, creá una vez este script en el servidor:

```bash
nano /root/actualizar.sh
```

Pegá:

```bash
#!/bin/bash
cd /root/Gaven
git pull
systemctl restart gaven
echo "Panel actualizado ✅"
```

Guardá y dale permiso de ejecución:

```bash
chmod +x /root/actualizar.sh
```

Desde ahí, cada vez que quieras actualizar, después de hacer `git push` en tu PC, solo corrés en el servidor:

```bash
/root/actualizar.sh
```

### Aclaraciones útiles

- **Cambios en el código del panel (app.py):** necesitás `git pull` + reiniciar (el script de arriba).
- **Cambios solo en los datos:** eso lo hace el cron solo, no tenés que tocar nada.
- **Si cambiás `requirements.txt`** (agregás una librería): después del `git pull` corré también
  `env/bin/pip install -r requirements.txt` antes de reiniciar.
- **El `secrets.toml` vive solo en el servidor** (no está en GitHub), así que un `git pull` nunca lo pisa. Tranquilo.

> *Más adelante*, si querés que se actualice automático con solo hacer `git push` (sin entrar
> al servidor), se puede con GitHub Actions o un webhook. Es un paso más avanzado; cuando
> domines lo de arriba, pedímelo y lo armamos.
