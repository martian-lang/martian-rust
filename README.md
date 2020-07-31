# Martian Rust Adapter


[![Build Status](https://img.shields.io/travis/martian-lang/martian-rust?logo=travis&style=for-the-badge)](https://travis-ci.org/martian-lang/martian-rust.svg?branch=master)
[![Documentation](https://img.shields.io/badge/Pages-Documentation-blue.svg?style=for-the-badge&logo=github)](http://martian-lang.github.io/martian-rust)

[Martian](https://martian-lang.org/) is a language and framework for developing and executing complex computational pipelines. The fundamental computational unit in martian is a stage, which is an entity that takes in a bunch of inputs and produces a bunch of outputs, optionally breaking the input space into chunks and parallelizing the computation. By design, the core computation in the stages can be written in any language, provided they implement an `adapter` which can help the language "talk" with martian. This crate implements a rust [adapter](https://martian-lang.org/writing-stages/) that will help you write martian stage code in Rust leveraging it's strong typing. By abstracting away the internals of the `adapter` and the way it communicates with martian, this crate provides a fairly high level `trait` based API for implementing stages.
