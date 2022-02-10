Authorization
==============

The iDDS currently supports both x509_proxy and oidc based authroization.

509_proxy
~~~~~~~~~~

x509_proxy based authorization is the default authorization method for iDDS.
It's implemented mainly based mod_ssl and mod_gridsite.

oidc
~~~~~~~~

.. image:: ../../images/v2/idds_authentication.jpg
         :alt: iDDS OIDC authorization

The iDDS OIDC authorization is based on the IAM service. Here are the steps for token initialization:

1. Get sign url: Get a sign url with a device code for users to approve.
2. User goes to the IAM service and approves the token request with the sign url.
3. Get the token with the device code.
4. The iDDS OIDC authorization service also includes services such as token refresh, token clean, token information checks and so on.


For normal iDDS requests, here are steps how iDDS authorize a users.

1. User initializes a normal request.
2. iDDS automatically finds the token and loads the token to headers of the http request.
3. Send the request to iDDS REST server.
4. iDDS server parse the token and verify the token against the IAM server. Verified users will be authorized.
