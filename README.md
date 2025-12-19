# pear-init

> Create initial Pear project files

## Usage

Interacts with the terminal prompt, asks users for inputs, stream emits data as input is received. Can only be used with a terminal.

```js
import init from 'pear-init'
```

```js
function status(info) {
  console.log(info)
}
const link = 'pear://....'
const stream = init(link, opts)
stream.on('data', status)
```

## License

Apache-2.0
