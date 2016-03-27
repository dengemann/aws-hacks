# Authors: Denis A. Engemann <denis.engemann@gmail.com>
#
# License: BSD (3-clause)

import os
import boto
import boto.s3.connection
from boto.s3.key import Key
from boto.ec2 import EC2Connection

_base_cmd_tmp = """#!/bin/bash
echo "updating code ..."
source /home/ubuntu/.bashrc
source /home/ubuntu/{anaconda_path}/bin/activate {env}
{install_pip}
{swapfile_cmd}
(cd /home/ubuntu/github/{repo} \\
  && git pull origin master \\
  && echo "updating code ... done" \\
  && {cmd})
"""

_base_swap_tmp = """echo "making swap file ..."
sudo chown ubuntu /mnt
sudo dd if=/dev/zero of=/mnt/swapfile bs=1M count={add_swap_file}
sudo chown root:root /mnt/swapfile
sudo chmod 600 /mnt/swapfile
sudo mkswap /mnt/swapfile
sudo swapon /mnt/swapfile
echo "/mnt/swapfile swap swap defaults 0 0" | sudo tee -a /etc/fstab
sudo swapon -a
echo "making swap file ... done"
"""

def make_start_script(cmd, repo, anaconda_path, env,
                      install_pip=(), add_swap_file=False):
    """ My basic startup template formatter

    Parameters
    ----------
    cmd : str
        The actual command to run.
    repo : str
        The repository
    anaconda_path : str
        The anaconda path on my AMI.
    env : str
        The anaconda environment.
    install_pip : list of str
        Some last-minute packages that are missing on my AMI.
    add_swap_file : bool, int
        Need a swapfile? No problem. Tell me your size.
    """
    swapfile_cmd = ''
    if add_swap_file:
        swapfile_cmd = _base_swap_tmp.format(add_swap_file=add_swap_file)
    if len(install_pip) == 0:
        install_pip = ''
    else:
        install_pip = '\n'.join(
            ['{anaconda_path}/bin/pip install {package}'.format(
             anaconda_path=anaconda_path, package=package)
             for package in install_pip])
    script = _base_cmd_tmp.format(
        anaconda_path=anaconda_path,
        install_pip=install_pip,
        swapfile_cmd=swapfile_cmd,
        repo=repo,
        env=env,
        cmd=cmd)
    return script


def download_from_s3(aws_access_key_id, aws_secret_access_key, bucket, fname,
                     key, dry_run=False,
                     host='s3.amazonaws.com'):
    """Download file from bucket
    """
    switch_validation = False
    if host is not None and not isinstance(
            host, boto.s3.connection.NoHostProvided):
        if 'eu-central' in host:
            switch_validation = True
            os.environ['S3_USE_SIGV4'] = 'True'

    com = boto.connect_s3(aws_access_key_id, aws_secret_access_key, host=host)
    bucket = com.get_bucket(bucket, validate=False)
    my_key = Key(bucket)
    my_key.key = key
    out = False
    if my_key.exists():
        if not dry_run:
            s3fid = bucket.get_key(key)
            s3fid.get_contents_to_filename(fname)
            out = True
        else:
            return True
    else:
        print('could not get %s : it does not exist' % key)
        out = False
    if switch_validation:
        del os.environ['S3_USE_SIGV4']
    return out


def upload_to_s3(aws_access_key_id, aws_secret_access_key, fname, bucket, key,
                 callback=None, md5=None, reduced_redundancy=False,
                 content_type=None, host='s3.eu-central-1.amazonaws.com'):
    """
    XXX copied from somewher on stackoverflow. Hope to find it again.

    Uploads the given file to the AWS S3
    bucket and key specified.

    callback is a function of the form:

    def callback(complete, total)

    The callback should accept two integer parameters,
    the first representing the number of bytes that
    have been successfully transmitted to S3 and the
    second representing the size of the to be transmitted
    object.

    Returns boolean indicating success/failure of upload.
    """
    switch_validation = False
    if host is not None:
        if 'eu-central' in host:
            switch_validation = True
            os.environ['S3_USE_SIGV4'] = 'True'
    com = boto.connect_s3(aws_access_key_id, aws_secret_access_key, host=host)
    bucket = com.get_bucket(bucket, validate=True)
    s3_key = Key(bucket)
    s3_key.key = key
    if content_type:
        s3_key.set_metadata('Content-Type', content_type)

    with open(fname) as fid:
        try:
            size = os.fstat(fname.fileno()).st_size
        except:
            # Not all file objects implement fileno(),
            # so we fall back on this
            fid.seek(0, os.SEEK_END)
            size = fid.tell()
        sent = s3_key.set_contents_from_file(
            fid, cb=callback, md5=md5, reduced_redundancy=reduced_redundancy,
            rewind=True)
        # Rewind for later use
        fid.seek(0)

    if switch_validation:
        del os.environ['S3_USE_SIGV4']

    if sent == size:
        return True
    return False


def get_run_parallel_script(parallel_args):
    """ script generator for run parallel
    It maps functions to command line arguments
    for run_parallel
    """
    parallel_cmd = ('python run_parallel.py %s' % ' '.join(
        ['--{param} {value}'.format(
            param=param,
            value=' '.join(value) if param == 'par_args' else value)
         for param, value in parallel_args.items()] 
    ))
    return parallel_cmd


def instance_run_jobs(code, image_id, key_name,
                      aws_access_key_id, aws_secret_access_key,
                      shutdown_behavior='terminate',
                      instance_type='t2.micro', dry_run=False, **kwargs):
    """ A simple wrapper around boto tools
    image_id : str
        The ID of the AMI.
    key_name : str
        The name of the ssh security keypair as you would see it in the AWS
        console.
    code : str
        The code that you want to run, essentially the contents of a script.
        Note that unlike the inputs to aws ec2 cli this does not accept
        files, it must be the file contents with a shebang at the top.
    aws_access_key_id : str
        The key.
    aws_secret_access_key : str
        The secret key.
    shutdown_behavior : str
        see `instance_initiated_shutdown_behavior` in boto.
    instance_type : str
        see `instance_type` in boto.
        But my favorites are:
            t2.micro : nice for testing
            c3.x2large : has SSDs on epehmeral0 and ephemeral1
            c3.x4large : 2 times c3.x2large
            c3.x8large : 4 c3.x2large
    dry_run : bool
        The dry run for testing.
    block_device_map : instance BlockDeviceMapping
        A particular dict-subclass from boto that maps the storage
        devices to mount points.

        This example
        
            mapping = BlockDeviceMapping()
            mapping["/dev/sdb"] = BlockDeviceType(ephemeral_name='ephemeral0')

        Is equivalent to this JSON syntax:
            [
              {
                "DeviceName": "/dev/sdb",
                "VirtualName": "ephemeral0"
              }
            ]
    """
    ec2con = EC2Connection(aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)

    out = ec2con.run_instances(
        image_id=image_id, key_name=key_name, instance_type=instance_type,
        user_data=code,
        instance_initiated_shutdown_behavior=shutdown_behavior,
        dry_run=dry_run, **kwargs)
    return out
