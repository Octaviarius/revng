#
# This file is distributed under the MIT License. See LICENSE.md for details.
#

# Keep opt-in narrow: other pass lists, global containers and model analyses
# remain sequential until independently compared with the native serial runner.
OPTIMIZATION_PASSES = (
    "dce",
    "remove-extractvalues",
    "simplify-cfg-with-hoist-and-sink",
    "dse",
    "instcombine",
    "remove-extractvalues",
    "sroa",
    "instsimplify",
    "jump-threading",
    "licm",
    "unreachableblockelim",
    "instcombine",
    "remove-extractvalues",
    "early-cse",
    "simplify-cfg-with-hoist-and-sink",
    "early-type-shrinking",
    "type-shrinking",
    "early-cse",
    "instsimplify",
    "gvn",
    "instsimplify",
    "dse",
    "dce",
    "drop-opaque-return-address",
    "globaldce",
)


def configure_native_parallel_pipes(pipes):
    settings = {
        "emit-c": ("Output", ("Modules",), False, False),
        "collect-cfg": ("Output", (), False, True),
        "isolate": ("Output", (), False, True),
        "clifter": ("Output", ("Input",), False, True),
        "pure-llvm-passes-pipe": ("Module", (), True, False),
    }
    for name, (output, inputs, in_place, conservative) in settings.items():
        if name not in pipes:
            continue
        pipe = pipes[name]
        pipe.parallel_output = output
        pipe.parallel_inputs = inputs
        pipe.parallel_in_place = in_place
        pipe.parallel_invalidate_on_model_change = conservative
        pipe.parallel_configurations = ()
        if name == "pure-llvm-passes-pipe":
            pipe.parallel_configurations = (OPTIMIZATION_PASSES,)
