Commander
=========

Process distribution platform for ques using 0mq


## Mac Installation

- Xcode is needed
- Command line Tools are needed (https://developer.apple.com/downloads/index.action?name=for%20Xcode%20-)
- zeromq IS needed
```shell
	brew install --devel zeromq
	brew link zeromq
```
	- If linking does not work FIX your permissions for brew to link :
```shell
	sudo chown -R $USER /usr/local/include
	sudo chown -R $USER /usr/local/lib
```
## Testing

```shell
	npm run test
```