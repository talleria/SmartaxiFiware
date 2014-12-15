#!/bin/sh

username=''
password=''
node_cdmi='130.206.82.9'
objectname=$2

curl -d '{"auth": {"passwordCredentials": {"username":"'$username'", "password":"'$password'"}}}' -H 'Content-type: aplication/json' http://cloud.lab.fi-ware.eu:4730/v2.0/tokens  > auth_token1.dat
token1=$(awk -F"[,:]" '{for(i=1;i<=NF;i++) {if($i~/id\042/) {print $(i+1)}  }  }' auth_token1.dat | awk -F'"' '{print $2; exit}')

curl -H 'x-auth-token: '$token1 \
      http://cloud.lab.fi-ware.eu:4730/v2.0/tenants \
      > auth_tenant.dat
tenantName=$(awk -F"[,:]" '{for(i=1;i<=NF;i++)
                       {if($i~/id\042/)
                         {print $(i+1)} 
                       }
                     }' auth_tenant.dat | awk -F'"' '{print $2; exit}')

curl \
    -d '{ "auth" : 
          { "passwordCredentials" : 
            { "username" : "'$username'" , "password" : "'$password'" },
          "tenantName" : "'$tenantName'" } 
        }' \
    -H "Content-Type: application/json" \
     http://cloud.lab.fi-ware.eu:4730/v2.0/tokens \
     > auth_token2.dat
token=$(awk -F"[,:]" '{for(i=1;i<=NF;i++)
                       {if($i~/id\042/)
                         {print $(i+1)} 
                       }
                     }' auth_token2.dat | awk -F'"' '{print $2; exit}')
auth=$(awk -F"[,:]" '{for(i=1;i<=NF;i++)
                      {if($i~/publicURL\042/)
                        {print $(i+3)}
                      }
                    }' auth_token2.dat | \
  grep  "v1/AUTH" | awk -F'"}]' '{print $1;}' | awk -F"/" '{print $3;}' )

myobject='CDMI_object_test_data.dat'
curl \
    -X PUT \
    -H 'X-Auth-Token: '$token \
    -H 'Content-Type: application/stream-octet' \
    -H 'Accept: */*' \
    --data-binary "@$1" \
    http://$node_cdmi:8080/cdmi/$auth/smartaxi/$objectname