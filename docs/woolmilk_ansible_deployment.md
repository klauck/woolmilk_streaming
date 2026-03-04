## Ansible Deployment for Distributed Execution

WoolMilk can be run on multiple remote machines using the `run_cluster.py` script with the `--mode remote` flag and a configuration for the remote cluster (see [`scripts/config_remote.json`](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/config_remote.json)).
The `run_cluster.py` script uses SSH to start WoolMilk processes on remote nodes and assumes that all required dependencies are already installed. In the cluster configuration, you need to specify:

- `username` — the SSH user on the remote machines  
- `base_dir` — the directory where WoolMilk sources are stored  
- `python_env` — the Python environment to use  

---

### Setting Up WoolMilk Sources and Python Environment

We provide an **Ansible script** to deploy WoolMilk sources and set up the Python environment on the cluster nodes.

The script requires:

- A **host inventory file** (see [`scripts/inventory_example.ini`](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/inventory_example.ini))  
- SSH authentication configured for the remote hosts  

Example SSH key setup for two nodes:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub picocluster@192.168.2.60
ssh-copy-id -i ~/.ssh/id_ed25519.pub picocluster@192.168.2.61
```


After configuring SSH, deploy the WoolMilk source files and set up the Python environment:

If needed, edit [`scripts/deployment.yml`](https://github.com/klauck/woolmilk_streaming/blob/main/scripts/deployment.yml)) to change the local source path (`project_src`), destination path on the cluster nodes (`project_dest`), and virtual environment location (`venv_path`).

 Then, run the deployment command:
```bash
cd scripts
ansible-playbook -i inventory_example.ini deployment.yml
cd ..
```

- For password authentication, you can provide: `ansible_password=password`.
- The playbook uses synchronize to efficiently copy many files, which is faster than standard copy tasks.

**Important Note**: The Nexmark binary is not installed by the Ansible scripts and must be installed manually on used *source* nodes.
