(function() {var implementors = {};
implementors["bincode"] = [{"text":"impl&lt;'storage&gt; Read for SliceReader&lt;'storage&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for IoReader&lt;R&gt;","synthetic":false,"types":[]}];
implementors["flate2"] = [{"text":"impl&lt;R:&nbsp;Read&gt; Read for CrcReader&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for DeflateEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for DeflateDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for DeflateEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for DeflateDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;W:&nbsp;Read + Write&gt; Read for DeflateEncoder&lt;W&gt;","synthetic":false,"types":[]},{"text":"impl&lt;W:&nbsp;Read + Write&gt; Read for DeflateDecoder&lt;W&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for GzEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for GzDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for MultiGzDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for GzEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for GzDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for MultiGzDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read + Write&gt; Read for GzEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;W:&nbsp;Read + Write&gt; Read for GzDecoder&lt;W&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for ZlibEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;BufRead&gt; Read for ZlibDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for ZlibEncoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;R:&nbsp;Read&gt; Read for ZlibDecoder&lt;R&gt;","synthetic":false,"types":[]},{"text":"impl&lt;W:&nbsp;Read + Write&gt; Read for ZlibEncoder&lt;W&gt;","synthetic":false,"types":[]},{"text":"impl&lt;W:&nbsp;Read + Write&gt; Read for ZlibDecoder&lt;W&gt;","synthetic":false,"types":[]}];
implementors["lz4"] = [{"text":"impl&lt;R:&nbsp;Read&gt; Read for Decoder&lt;R&gt;","synthetic":false,"types":[]}];
implementors["rand_core"] = [{"text":"impl Read for dyn RngCore","synthetic":false,"types":[]}];
implementors["tempfile"] = [{"text":"impl Read for NamedTempFile","synthetic":false,"types":[]},{"text":"impl&lt;'a&gt; Read for &amp;'a NamedTempFile","synthetic":false,"types":[]},{"text":"impl Read for SpooledTempFile","synthetic":false,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()