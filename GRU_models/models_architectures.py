from pipeline_architecture import Model
models = [
    Model("./GRU_models/Layer 2/GRU_1min.h5"),
    Model("./GRU_models/Layer 1/GRU_2min.h5"),
    Model("./GRU_models/Layer 2/GRU_3min.h5"),
    Model("./GRU_models/Layer 1/GRU_4min.h5"),
    Model("./GRU_models/Layer 1/GRU_5min.h5")
]

for _model_ in models:
    print(_model_.model.summary())
    print()