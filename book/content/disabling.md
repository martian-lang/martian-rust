# Disabling a Stage


A stage can be disabled by specifiying a boolean flag in the `using` section of the mro. For example:

```mro
stage MY_STAGE(
...
) using (
    disabled  = self.no_bam,
)
```

Since we will be auto-generating the mro, we should not be editing the mro directly. Instead we should modify the proc-macro attribute `#[make_mro]` to include the disabled flag. i.e:

```rust
#[make_mro(disabled=self.nobam)]
impl MartianStage for MyStage {
......
}
```

Note that the text after disabled will be output directly and can refer to any value that will be used in the pipeline and must be set based on your knowledge of the pipeline.  If a stage is used in multiple pipelines, the `disabled` variable should be available in both.