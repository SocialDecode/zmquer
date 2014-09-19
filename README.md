Commander
=========

Process distribution platform for ques using 0mq


## Mac Installation

- Xcode is needed
- Command line Tools are needed : https://developer.apple.com/downloads/index.action?name=for%20Xcode%20-
- zeromq is needed

    brew install pkg-config
    brew install --devel zeromq
    brew link zeromq (only if link fails)

- If linking still does not work FIX your permissions for brew to link :  


      sudo chown -R $USER /usr/local/include
      sudo chown -R $USER /usr/local/lib


## Testing

  npm run test


## For Http Server
`config/[name].htpasswd` file is needed named the same way as  
declared on `config/defaults-server.json` or, if exists, `config/server.json`.  

A `config/users.htpasswd.sample` is provided and can be renamed to start using it.  
The default username is **admin** and password is **admin**