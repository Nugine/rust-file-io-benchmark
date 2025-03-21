# rust-upload-file-benchmark

Task: Save a large file to disk from PUT request.

Goal: Highest transmission speed.

## Run benchmark

Create a 1GiB file with random data.

```bash
just mkdata
```

Start the server

```bash
just server
```

Run the client

```bash
just client
```

See [client.log](./client.log) for the results.

See [src/routes.rs](src/routes.rs) for the methods.
