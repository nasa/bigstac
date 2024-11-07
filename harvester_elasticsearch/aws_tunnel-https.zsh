#!/bin/zsh

_fetch_rds() {
    output=$(aws rds describe-db-instances \
            --output text \
            --query "DBInstances[].Endpoint.Address" \
            --profile $1)
    read -A output_arr <<< $output
    if [ "${#output_arr[@]}" -gt 1 ]; then
        PS3="Please select database: "
        select lb in $output_arr
        do
            echo $lb
            break
        done
    else
        echo ${output_arr[1]}
    fi
}

_fetch_elb() {
    output=$(aws elb describe-load-balancers \
            --output text \
            --query "LoadBalancerDescriptions[].DNSName" \
            --profile $1)
    read -A output_arr <<< $output
    if [ "${#output_arr[@]}" -gt 1 ]; then
        PS3="Please select load balancer: "
        select lb in $output_arr
        do
            echo $lb
            break
        done
    else
        echo ${output_arr[1]}
    fi
}

_fetch_elbv2() {
    output=$(aws elbv2 describe-load-balancers \
                --query "LoadBalancers[].DNSName" \
                --output text \
                --profile $1)
    read -A output_arr <<< $output
    if [ "${#output_arr[@]}" -gt 1 ]; then
        PS3="Please select load balancer: "
        select lb in $output_arr
        do
            echo $lb
            break
        done
    else
        echo ${output_arr[1]}
    fi
}

_fetch_jumpbox() {
    output=$(aws ec2 describe-instances \
            --filter "Name=tag:Name,Values=*SSM Jumpbox*" \
            --query "Reservations[].Instances[?State.Name == 'running'].InstanceId[]" \
            --output text \
            --max-items 1 \
            --profile $1)
    echo $output
}

tunnel() {
  if [ $# -eq 0 ]; then
    echo "Usage: tunnel <env> [<target_service>]"
  fi

  case $1 in
    sit)
        AWS_PROFILE="cmr-sit"
        ;;
    uat)
        AWS_PROFILE="cmr-uat"
        ;;
    prod)
        AWS_PROFILE="cmr-prod"
        ;;
    (wl|sandbox))
        AWS_PROFILE="cmr-wl"
        ;;
    *)
        echo "Invalid environment specified"
        return -1
        ;;
  esac

  case $2 in
      (db|database|oracle))
        TARGET=$(_fetch_rds $AWS_PROFILE)
        LOCAL_PORT=1522
        REMOTE_PORT=1521
        ;;
    (es|elastic*))
        TARGET=$(_fetch_elb $AWS_PROFILE)
        LOCAL_PORT=9201
        REMOTE_PORT=9200
        ;;
    *)
        TARGET=$(_fetch_elbv2 $AWS_PROFILE)
        LOCAL_PORT=8080
        REMOTE_PORT=443
        ;;
  esac

  JUMP_BOX=$(_fetch_jumpbox $AWS_PROFILE)

  echo "Jumpbox: $JUMP_BOX"

  ssh -i ~/.ssh/id_rsa -f -N \
      -L $LOCAL_PORT:$TARGET:$REMOTE_PORT ec2-user@$JUMP_BOX \
      -o ProxyCommand="aws ssm start-session \
                        --target %h \
                        --document-name AWS-StartSSHSession \
                        --parameters 'portNumber=%p' \
                        --profile $AWS_PROFILE" \
      -o ServerAliveInterval=60 \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=/dev/null

  printf "ssh tunnel open through $JUMP_BOX\n"
  printf " - with profile $AWS_PROFILE\n"
  printf " - with forwarding $LOCAL_PORT:$TARGET:$REMOTE_PORT\n\n"
  printf "http://localhost:$LOCAL_PORT\n\n"
  printf "close with:\n - pkill ssh\n - kill $(ps aux | grep ssh | grep $TARGET | awk '{print $2}')\n"
}
