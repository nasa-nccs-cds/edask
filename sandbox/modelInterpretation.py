import numpy as np
from scipy.stats import percentileofscore
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from glob import glob
from os.path import join, expanduser
import os

from keras.models import Model, save_model, load_model
from keras.layers import Dense, Activation, Conv2D, Input, AveragePooling2D, Flatten, LeakyReLU
from keras.layers import Dropout, BatchNormalization
from keras.regularizers import l2
from keras.optimizers import SGD, Adam
import keras.backend as K
# from IPython.display import SVG
from keras.utils.vis_utils import model_to_dot

session = K.tf.Session(config=K.tf.ConfigProto(allow_soft_placement=True,
                                               gpu_options=K.tf.GPUOptions(allow_growth=True),
                                               log_device_placement=False))

K.set_session(session)
# One of the common critiques of machine learning models is that many appear to be a "black box", in which predictions are made without associated reasoning why they were made. However, there are ways to interrogate machine learning models to extract information about how the model weights each input and how the model encodes information internally.
# Permutation variable importance is a model-agnostic way of estimating how much each input affects the prediction values on average. In permutation variable importance, a metric is calculated on the model predictions of the validation data. Then each validation set input is permuted, or shuffled, among the examples, and the metric is recalculated. A large change in the metric value corresponds with higher importance for that input.

def normalize_multivariate_data(data, scaling_values=None):
    """
    Normalize each channel in the 4 dimensional data matrix independently.

    Args:
        data: 4-dimensional array with dimensions (example, y, x, channel/variable)
        scaling_values: pandas dataframe containing mean and std columns

    Returns:
        normalized data array, scaling_values
    """
    normed_data = np.zeros(data.shape, dtype=data.dtype)
    scale_cols = ["mean", "std"]
    if scaling_values is None:
        scaling_values = pd.DataFrame(np.zeros((data.shape[-1], len(scale_cols)), dtype=np.float32),
                                      columns=scale_cols)
    for i in range(data.shape[-1]):
        scaling_values.loc[i, ["mean", "std"]] = [data[:, :, :, i].mean(), data[:, :, :, i].std()]
        normed_data[:, :, :, i] = (data[:, :, :, i] - scaling_values.loc[i, "mean"]) / scaling_values.loc[i, "std"]
    return normed_data, scaling_values

def variable_importance(model, data, labels, input_vars, score_func, num_iters=10):
    preds = model.predict(data)[:, 0]
    score_val = score_func(labels, preds)
    indices = np.arange(data.shape[0])
    imp_scores = np.zeros((len(input_vars), num_iters))
    shuf_data = np.copy(data)
    for n in range(num_iters):
        print(n)
        np.random.shuffle(indices)
        for v, var in enumerate(input_vars):
            print(var)
            shuf_data[:, :, :, v] = shuf_data[indices, :, :, v]
            shuf_preds = model.predict(shuf_data)[:, 0]
            imp_scores[v, n] = score_func(labels, shuf_preds)
            shuf_data[:, :, :, v] = data[:, :, :, v]
    return score_val - imp_scores

# conv_imp_scores = variable_importance(conv_model, norm_in_data[test_indices],
#                                        vort_labels[test_indices], in_vars, brier_skill_score, num_iters=3)

# We can visualize what input most activates the output layer of the network through feature optimization. Example code is shown below.

def feature_optimization( conv_model, in_vars, scaling_values ):
    out_diff = K.mean((conv_model.layers[-1].output - 1) ** 2)
    grad = K.gradients(out_diff, [conv_model.input])[0]
    grad /= K.maximum(K.sqrt(K.mean(grad ** 2)), K.epsilon())
    iterate = K.function([conv_model.input, K.learning_phase()],
                         [out_diff, grad])
    # input_img_data = np.random.normal(scale=0.1, size=(1, 32, 32, len(in_vars)))
    input_img_data = np.zeros(shape=(1, 32, 32, len(in_vars)))

    out_losses = []
    for i in range(20):
        out_loss, out_grad = iterate([input_img_data, 0])
        input_img_data -= out_grad * 0.1
        out_losses.append(out_loss)

    plt.figure(figsize=(8, 8))
    plt.pcolormesh(input_img_data[0, :, :, 0] * scaling_values.loc[0, "std"] + scaling_values.loc[0, "mean"],
                   vmin=-10, vmax=80, cmap="gist_ncar")
    plt.colorbar()
    plt.quiver(input_img_data[0, :, :, -2] * scaling_values.loc[1, "std"] + scaling_values.loc[1, "mean"],
               input_img_data[0, :, :, -1] * scaling_values.loc[2, "std"] + scaling_values.loc[2, "mean"],
               scale=500)
    plt.title("Mesocyclone Conv Net Output Layer Feature Optimization")
    plt.savefig("conv_model_output_feature.png", dpi=200, bbox_inches="tight")

    plt.figure(figsize=(8, 8))
    plt.pcolormesh(input_img_data[0, :, :, 0], vmin=-5, vmax=5, cmap="RdBu_r")
    plt.quiver(input_img_data[0, :, :, 1], input_img_data[0, :, :, 2], scale=100)

    return out_losses


def plot_model_weight_distribution(conv_model):
    plt.hist(conv_model.layers[-2].get_weights()[0], bins=50)
    plt.xlabel("Conv Net Output Layer Weights")
    plt.ylabel("Frequency")

def plot_max_activation_pattern(conv_model, norm_in_data,all_in_data,all_valid_times):
    weights = conv_model.layers[-2].get_weights()[0].ravel()
    num_top = 16
    num_ex = 9
    batch_size = 2048
    top_neurons = weights.argsort()[::-1][0:num_top]
    top_examples = np.zeros((num_top, num_ex), dtype=int)
    top_gradients = np.zeros((num_top, num_ex, 32, 32, 3))
    batch_i = list(range(0, norm_in_data.shape[0], batch_size)) + [norm_in_data.shape[0]]
    for n, neuron in enumerate(top_neurons):
        print(n, neuron, weights[neuron])
        act_func = K.function([conv_model.input, K.learning_phase()], [conv_model.layers[-3].output[:, neuron]])
        loss = 0.5 * (conv_model.layers[-3].output[:, neuron] - 4) ** 2
        grads = K.gradients(loss, conv_model.input)[0]
        grads /= K.maximum(K.std(grads), K.epsilon())
        grad_func = K.function([conv_model.input, K.learning_phase()], [grads])
        act_values = np.zeros(norm_in_data.shape[0])
        for b in range(len(batch_i) - 1):
            act_values[batch_i[b]:batch_i[b + 1]] = act_func([norm_in_data[batch_i[b]:batch_i[b + 1]], 0])[0]
        top_examples[n] = act_values.argsort()[::-1][0:num_ex]
        top_gradients[n] = -grad_func([norm_in_data[top_examples[n]], 0])[0]

    fig, axes = plt.subplots(4, 4, figsize=(16, 16), sharex=True, sharey=True)
    plt.subplots_adjust(0.01, 0.01, 0.95, 0.94, hspace=0, wspace=0)
    for a, ax in enumerate(axes.ravel()):
        ax.pcolormesh(all_in_data[top_examples[a, 0], :, :, 0], vmin=-10, vmax=80, cmap="gist_ncar")
        ax.contour(top_gradients[a, 0, :, :, 0], np.arange(1, 8, 2), cmap="Blues", linewidths=3)

        ax.quiver(all_in_data[top_examples[a, 0], :, :, 1], all_in_data[top_examples[a, 0], :, :, 2])
        ax.text(2, 2, "W: {0:0.2f} N: {1:02d} ".format(weights[top_neurons[a]], top_neurons[a]) + pd.Timestamp(
            all_valid_times[top_examples[a, 0]]).strftime("%Y-%m-%d H%H"), fontsize=14, color="k",
                bbox=dict(boxstyle="square", ec="k", fc="0.8"))
        ax.set_xticks(np.arange(8, 32, 8))
        ax.set_xticklabels(np.arange(8, 32, 8) * 3, fontsize=14)
        ax.set_yticks(np.arange(8, 32, 8))
        ax.set_yticklabels(np.arange(8, 32, 8) * 3, fontsize=14)
    plt.suptitle("Top Neuron Activations", fontsize=16)
    plt.savefig("top_neuron_acts.png", dpi=200, bbox_inches="tight")

    fig, axes = plt.subplots(3, 3, figsize=(16, 16), sharex=True, sharey=True)
    plt.subplots_adjust(0.01, 0.01, 0.95, 0.95, hspace=0, wspace=0)
    pn = 6
    for a, ax in enumerate(axes.ravel()):
        ax.pcolormesh(all_in_data[top_examples[pn, a], :, :, 0], vmin=-10, vmax=80, cmap="gist_ncar")
        ax.contour(top_gradients[pn, a, :, :, 0], np.arange(1, 8, 2), cmap="Blues", linewidths=3)
        ax.quiver(all_in_data[top_examples[pn, a], :, :, 1], all_in_data[top_examples[pn, a], :, :, 2])
        ax.text(2, 2, pd.Timestamp(all_valid_times[top_examples[pn, a]]).strftime("%Y-%m-%d H%H"), fontsize=16,
                bbox=dict(boxstyle="square", ec="k", fc="0.8"))
        ax.set_xticks(np.arange(8, 32, 8))
        ax.set_xticklabels(np.arange(8, 32, 8) * 3, fontsize=14)
        ax.set_yticks(np.arange(8, 32, 8))
        ax.set_yticklabels(np.arange(8, 32, 8) * 3, fontsize=14)
    plt.suptitle("Neuron {0:02d} Top Examples".format(top_neurons[pn]), fontsize=16)
    plt.savefig("neuron_{0:02d}_top_examples.png".format(top_neurons[pn]), dpi=200, bbox_inches="tight")