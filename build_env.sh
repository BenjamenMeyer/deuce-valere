#!/bin/bash

SHOW_HELP="no"
REBUILD_ENV="no"
CREATE_VAULT="no"
DELETE_VAULT="no"
PREPARE_ONLY="no"
VALIDATE_ONLY="no"
CLEANUP_ONLY="no"
GENERATE_DATA="yes"
LIST_VAULT="no"

AUTH_SERVICE='rackspace'
USER_DATA='/home/bmeyer/rackspace/users/users-quattrodev.json'
DC='iad'
DEUCE_SERVER='192.168.3.1:80'

VAULT_NAME='brm_valere_foo5'

BLOCK_COUNT=20
CACHE_EXPIRATION=3600
DATA_EXPIRATION=1

for arg in $@
do
	if [ "${arg}" == "--help" ]; then
		SHOW_HELP="yes"
	elif [ "${arg}" == "-h" ]; then
		SHOW_HELP="yes"
	elif [ "${arg}" == "--rebuild" ]; then
		REBUILD_ENV="yes"
	elif [ "${arg}" == "-r" ]; then
		REBUILD_ENV="yes"
	elif [ "${arg}" == "--create" ]; then
		CREATE_VAULT="yes"
	elif [ "${arg}" == "-c" ]; then
		CREATE_VAULT="yes"
	elif [ "${arg}" == "--delete" ]; then
		DELETE_VAULT="yes"
	elif [ "${arg}" == "-d" ]; then
		DELETE_VAULT="yes"
	elif [ "${arg}" == "--prepare-only" ]; then
		PREPARE_ONLY="yes"
		GENERATE_DATA="no"
	elif [ "${arg}" == "--validate-only" ]; then
		VALIDATE_ONLY="yes"
		CLEANUP_ONLY="no"
	elif [ "${arg}" == "--cleanup-only" ]; then
		VALIDATE_ONLY="no"
		CLEANUP_ONLY="yes"
	elif [ "${arg}" == "--skip-data-generation" ]; then
		GENERATE_DATA="no"
    elif [ "${arg}" == "--file-list-vault-only" ]; then
        LIST_VAULT="files"
    elif [ "${arg}" == "--block-list-vault-only" ]; then
        LIST_VAULT="blocks"
	fi
	
done

if [ "${SHOW_HELP}" == "yes" ]; then
    echo "${0} [--rebuild] [-r] [--create] [-c] [--delete] [-d] [--cleanup-only] [--prepare-only] [--validate-only] [--skip-data-generation] [--help] [-h]"
    echo
	echo "  --cleanup-only            only perform the cleanup operation"
	echo "  --create		          create the vault"
	echo "  --delete                  delete the vault"
    echo "  --help                    show this message"
	echo "  --prepare-only            only perform the preparation (create/destroy) steps"
    echo "  --rebuild                 rebuild the virtual environment"
	echo "  --skip-data-generation    skip adding data to the vault"
	echo "  --validate-only           only perform the validation operation"
    echo "  --file-list-vault-only    only list the files in the vault"
    echo "  --block-list-vault-only   only list the blocks in the vault"
	echo
	echo "  -c                        alias for --create"
	echo "  -d                        alias for --delete"
    echo "  -r                        alias for --rebuild"
    echo "  -h                        alias for --help"
    exit 0
fi

if [ "${REBUILD_ENV}" == "yes" ]; then
	echo "Rebuilding environment..."
    if [ -d env ]; then
        rm -Rf env
    fi

    virtualenv -p `which python3` env
    source env/bin/activate
    pip install . --pre deuce-client

else
    source env/bin/activate

fi

run_valere()
	{
	if [ "${PREPARE_ONLY}" == "yes" ]; then
		return 0
	fi

	if [ "${CLEANUP_ONLY}" == "no" ]; then
		echo "Validating Vault"
		deucevalere --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} --vault-name ${VAULT_NAME} --cache-expiration ${CACHE_EXPIRATION} --data-expiration ${DATA_EXPIRATION} validate
	fi

	if [ "${VALIDATE_ONLY}" == "no" ]; then
		if [ "${CLEANUP_ONLY}" == "no" ]; then
			sleep 1
		fi
		echo "Cleaning up Vault"
		deucevalere --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} --vault-name ${VAULT_NAME} --cache-expiration ${CACHE_EXPIRATION} --data-expiration ${DATA_EXPIRATION} cleanup
	fi
	}

create_vault()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} vault --vault-name ${VAULT_NAME} create
	}

detect_vault()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} vault --vault-name ${VAULT_NAME} exists
	local -i retval=$?
	return ${retval}
	}

delete_vault()
	{
    detect_vault
    if [ $? -eq 0 ]; then
        echo "	Deleting files..."
        delete_files
        echo "	Deleting blocks..."
        delete_blocks
        echo "	Cleaning up Vault..."
        run_valere
        echo "	Deleting the Vault..."
        deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} vault --vault-name ${VAULT_NAME} delete
    else
        echo "  Vault does not exist. Unable to cleanup"
    fi
	}

upload_file()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} files --vault-name ${VAULT_NAME} upload --content ${1}
    return $?
	}

file_list()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} files --vault-name ${VAULT_NAME} list 
    return $?
	}

delete_file()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} files --vault-name ${VAULT_NAME} delete --file-id ${1} 
    return $?
	}

upload_and_delete_file()
	{
	local FILE_ID=`deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} files --vault-name ${VAULT_NAME} upload --content ${1} | grep "File ID" | cut -f 2 -d ':'`

	delete_file ${FILE_ID}
    return $?
	}

delete_files()
	{
	for file_id in `deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} files --vault-name ${VAULT_NAME} list | tr -s '\t' ';' | grep '^;' | cut -f 2 -d ';'`
	do
		delete_file ${file_id}
	done
    return 0
	}

upload_block()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} blocks --vault-name ${VAULT_NAME} upload --block-content ${1}
    return $?
	}

delete_block()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} blocks --vault-name ${VAULT_NAME} delete --block-id ${1}
    return $?
	}

block_list()
	{
	deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} blocks --vault-name ${VAULT_NAME} list 
    return $?
	}

delete_blocks()
	{
	for block_id in `deuceclient --user-config ${USER_DATA} --url ${DEUCE_SERVER} -dc ${DC} --auth-service ${AUTH_SERVICE} blocks --vault-name ${VAULT_NAME} list  | tr -s '\t' ';' | grep '^;' | cut -f 2 -d ';'`
	do
		delete_block ${block_id}
	done
    return 0
	}

make_expired_data()
	{
	upload_and_delete_file ChangeLog
    return $?
	}

make_orphaned_data()
	{
	upload_file AUTHORS
	upload_file LICENSE
	for x in $(eval echo "{1..${BLOCK_COUNT}}")
	do
		echo "	Uploading orphaned copies ${x} of ${BLOCK_COUNT}"
		upload_block AUTHORS
		upload_block LICENSE
	done
    return 0
	}

if [ "${LIST_VAULT}" == "files" ]; then
    file_list
    exit $?
elif [ "${LIST_VAULT}" == "blocks" ]; then
    block_list
    exit $?
fi

if [ "${DELETE_VAULT}" == "yes" ]; then
	echo "Cleaning up the existing vault..."
	delete_vault
fi

if [ "${CREATE_VAULT}" != "yes" ]; then
	echo "Attempting to detect Vault - ${VAULT_NAME}..."
	detect_vault
	if [ $? -eq 0 ]; then
		echo "Vault exists."
	else
		echo "Vault does not exist. Marking it to be created"
		CREATE_VAULT="yes"
	fi
fi

if [ "${CREATE_VAULT}" == "yes" ]; then
	create_vault
fi

echo "Testing Empty Vault"
run_valere

if [ "${GENERATE_DATA}" == "yes" ]; then

	echo "Files"
	echo "--------------------------------"
	file_list
	echo

	echo "Blocks"
	echo "--------------------------------"
	block_list
	echo

	echo "Create expired data"
	make_expired_data
	block_list

	echo "Create orphaned data"
	make_orphaned_data
	block_list

	echo "Files"
	echo "--------------------------------"
	file_list
	echo

	echo "Blocks"
	echo "--------------------------------"
	block_list
	echo
fi

echo "Cleanup orphaned data"
run_valere
