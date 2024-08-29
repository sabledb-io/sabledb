
# Common for both client and server machines

```bash
sudo yum install gcc-g++ git clang -y
```

---
 Steps for building SableDB
---

# install rust

```bash
cd $HOME
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Add rust to the path

```bash
echo 'export PATH=$PATH:$HOME/.cargo/bin' >> ${HOME}/.bashrc
```

## source the bashrc

```bash
. ${HOME}/.bashrc
```

# Checkout the sources

```bash
ssh-keygen -t rsa -C "AWS SableDB"
cat ~/.ssh/id_rsa.pub
```

Add the ssh to git repo: `https://github.com/settings/keys`

## build sabledb

```bash
git clone https://github.com/sabledb-io/sabledb.git
cd sabledb
cargo build --release
```

## Run sabledb

```bash
cp server.ini conf.ini
rm -fr sabledb.db/ && target/release/sabledb conf.ini
```
