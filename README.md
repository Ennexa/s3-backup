## Remote Backup

Backup directories to Amazon S3 daily.

### Dependencies

Backup to S3 requires `rclone` to be installed.

    curl https://rclone.org/install.sh | sudo bash

Local backup depends on `rsync`, which can be installed using the OS package manager.

### Usage

    ./backup.py -c config.yaml

### Retention

- Daily backups for a week
- Weekly backups for a month
- Monthly backups for an year
- Yearly backups for ever
