<a href="https://quackpipe.fly.dev" target="_blank"><img src="https://github.com/user-attachments/assets/3223f0dd-78e2-4b61-8690-5502a2290421" width=220 /></a>

> _a pipe for quackheads_

# :baby_chick: quackpy

_QuackPy is a serverless OLAP API built on top of DuckDB emulating and aliasing the ClickHouse HTTP API_


## :hatched_chick: Demo
:hatched_chick: try a [sample s3/parquet query](https://quackpy.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCAxMA==) in our [miniature playground](https://quackpy.fly.dev) _(fly.io free tier, 1x-shared-vcpu, 256Mb)_ <br>
:hatched_chick: launch your own _free instance_ on [fly.io](https://flyctl.sh/shell?repo=quackscience/quackpy)


### :seedling: User-Interface
quackpy ships with the quack user-interface to execute SQL queries

![Quack-UI-Workspace-ezgif com-crop](https://github.com/user-attachments/assets/902a6336-c4f4-4a4e-85d5-78dd62cb7602)

<br>

### :seedling: Get Started
Run using [docker](https://github.com/quackscience/quackpy/pkgs/container/quackpy) or build from source

#### 🐋 Using Docker
```bash
docker pull ghcr.io/quackscience/quackpy:latest
docker run -ti --rm -p 8123:8123 ghcr.io/quackscience/quackpy:latest
```







###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^3]
