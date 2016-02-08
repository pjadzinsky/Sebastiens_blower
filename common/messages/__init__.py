from Pubnub import Pubnub

# These should be moved to a proper config area.
KEYS = {
    'mousera-staging':   [
        "pub-c-353c0d6b-8f41-4c85-9b62-3eda80a3c764", ## PUBLISH_KEY
        "sub-c-10a8744c-c42d-11e3-ab7b-02ee2ddab7fe", ## SUBSCRIBE_KEY
        "sec-c-OGQ0MGIxYjctOWRlOS00YjQzLTlhOTctNTA1MWRjYzE3NjA1"    ## SECRET_KEY
    ],
    'mousera-production':[
        "pub-c-9c3a47c6-43d8-409d-9b3f-6aa8da083b79",  ## PUBLISH_KEY
        "sub-c-bf9ff740-c42d-11e3-b872-02ee2ddab7fe",  ## SUBSCRIBE_KEY
        "sec-c-ZmI5MDg2NGEtZmU2My00MjlmLTlmOTgtMjI5NTZhOTE5NDg0"    ## SECRET_KEY
    ]
}
# TODO: do this configging properly and put it somewhere nice...
# The issue is that
def get_pubnub(env, pubsub):
    pub = sub = False
    if 'pub' in pubsub: pub = True
    if 'sub' in pubsub: sub = True
    keys = KEYS[env]

    return Pubnub(
        keys[0] if pub else None,  ## PUBLISH_KEY
        keys[1] if sub else None,  ## SUBSCRIBE_KEY
        keys[2],    ## SECRET_KEY
        False    ## SSL_ON?
    )

