
# Common for both client and server machines

```bash
sudo yum install gcc-g++ git clang -y
```

---
 Steps for building SableDb
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
ssh-keygen -t rsa -C "AWS SableDb"
cat ~/.ssh/id_rsa.pub
```

Add the ssh to git repo: `https://github.com/settings/keys`

## build sabledb

```bash
git clone git@github.com:eranif/sabledb.git
cd sabledb
cargo build --release
```

## Run sabledb (default setup: WAL enabled, 64MB of cache)

```bash
rm -fr sabledb.db/ && target/release/sabledb
```

# Build Redis (client machine)

```bash
cd ~
git clone https://github.com/redis/redis.git
cd redis
make -j$(nproc)
cd src/
```
