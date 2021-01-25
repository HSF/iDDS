=================
Contributor Guide
=================

* Thank you for participating!
* Below are the guids about how to contribute to it.

The `repository <https://github.com/HSF/iDDS>`_  consists of different branches:
 * the **master** branch includes the main stable developments for the next major version.
 * the **dev** branch includes latest developments. A contribution should be based on the dev branch.
 * the **<version>** branch includes deployments for different versions.


Generally all `pull requests <https://github.com/HSF/iDDS/pulls>`_ are to be created against the iDDS **dev** branch. Contributions will end up in the upstream **dev** when merged.


Getting started
---------------

**Step 1**: Fork the `repository <https://github.com/HSF/iDDS/>`_ on Github.

**Step 2**: Clone the repository to your development machine and configure it::

    $ git clone https://github.com/<YOUR_USER>/iDDS/
    $ cd iDDS
    $ git remote add upstream https://github.com/HSF/iDDS.git

**Step 3**: Setup local dev environment(The virtual environment is based on conda)::

    $ # If you are not in the idds top directory.
    $ # cd iDDS
    $ CurrentDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    $ CondaDir=${CurrentDir}/.conda/iDDS
    $ mkdir -p $CondaDir

    $ # For your local development, you may do not need all packages. In this case, you may need to comment out some packages, for example cx_Oracle.
    $ echo conda env create --prefix=$CondaDir  -f=main/tools/env/environment.yml
    $ conda env create --prefix=$CondaDir  -f=main/tools/env/environment.yml

**Step 4**: Configure local environment::

    $ # If you are not in the idds top directory.
    $ # cd iDDS
    $ RootDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    $ CondaDir=${RootDir}/.conda/iDDS
    $ export IDDS_HOME=$RootDir
    $ conda activate $CondaDir

Contributing
------------

**Step 1**: Developing based on your personal repository(if you already have some codes and want to keep them, you need to use 'git rebase'. In this case, you may need to fix some conflicts; If you start from scratch, you can directly use 'git reset'. This method will overwrite local changes. Becareful to backup if you are not confident about using it)::

    $ git checkout dev
    $ # echo "Updating dev"
    $ git pull --all --prune --progress
    $ echo "Rebasing dev"
    $ # git rebase upstream/dev dev
    $ git reset --hard upstream/dev

**Step 2**: Check your codes and fix the codes(flake8 is installed when you configure your virtual environments)::

    $ flake8 yourcodes.py
    $ flake8 */lib/idds/

**Step 3**: Commit your change. The commit command must include a specific message format::

    $ git commit -m "...."
    $ git push origin dev

**Step 4**: Create the pull request to the **dev** branch of HSF/iDDS repository.

While using the `github interface <https://help.github.com/articles/creating-a-pull-request/>`_ is the default interface to create pull requests, you could also use GitHub's command-line wrapper `hub <https://hub.github.com>`_ or the `GitHub CLI <https://cli.github.com/>`_.

**Step 5**: Watch the pull request for comments and reviews. If there are some conflicts, you may need to rebase your codes and fix the conflicts. For any pull requests update, please try to squash/amend your commits to avoid "in-between" commits::

    $ git rebase upstream/dev dev


Human Review
------------

Anyone is welcome to review merge requests and make comments!

The development team can approve, request changes, or close pull requests. Merging of approved pull requests is done by the iDDS development lead.


Coding Style
------------

We use flake8 to sanitize our code. Please do the same before submitting a pull request.
