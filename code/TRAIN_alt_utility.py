
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import time
import util_nn_mlarch as util
from sklearn import preprocessing


# starting time
start_time = time.time()

# #%matplotlib inline
# df_sp_train = pd.read_csv('data/data_AV_Singapore_v1_sp_train.csv')
# df_sp_validation = pd.read_csv('data/data_AV_Singapore_v1_sp_validation.csv')
# # here we combine train and validation set to recreate training and validation sets...
# df_sp_combined_train = pd.concat([df_sp_train, df_sp_validation], axis = 0)
# df_sp_combined_train.index = np.arange(df_sp_combined_train.shape[0])
# df_sp_test = pd.read_csv('data/data_AV_Singapore_v1_sp_test.csv')

df = pd.read_csv('train_set.csv')
dfTruth = pd.read_csv('truth.csv')
# 打乱数据集顺序
np.random.seed(100) # replicable
n_index = df.shape[0]
n_index_shuffle = np.arange(n_index)
np.random.shuffle(n_index_shuffle)

data_shuffled = df 
data_shuffled = df.iloc[n_index_shuffle, :]

data_shuffled_Truth = dfTruth[['od_ratio1', 'od_ratio2', 'od_ratio3', 'od_ratio4', 'od_ratio5']]
data_shuffled_Truth = data_shuffled_Truth.iloc[n_index_shuffle, :]
data_shuffled_Truth.fillna(0, inplace=True)

useful_vars = ['transfer_time1', 'avg_time1', 'full_price1', 'via_stations1', 'hourly_cnt1',
               'transfer_time2', 'avg_time2', 'full_price2', 'via_stations2', 'hourly_cnt2',
               'transfer_time3', 'avg_time3', 'full_price3', 'via_stations3', 'hourly_cnt3',
               'transfer_time4', 'avg_time4', 'full_price4', 'via_stations4', 'hourly_cnt4',
               'transfer_time5', 'avg_time5', 'full_price5', 'via_stations5', 'hourly_cnt5']

static_vars = ['hour_', 'normal_ratio', 'eld_ratio']

data_shuffled_useful_vars = data_shuffled[useful_vars]
# data_shuffled_useful_vars.dropna(axis = 0, how = 'any', inplace = True)

data_shuffled_static_vars = data_shuffled[static_vars]

# 数据集正则化
X = preprocessing.scale(data_shuffled_useful_vars.values)
X_static = preprocessing.scale(data_shuffled_static_vars.values)
Y = data_shuffled_Truth.values

def generate_cross_validation_set(data, validation_index, df = True):
    '''
    five_fold cross validation
    return training set and validation set
    df: True (is a dataframe); df: False: (is a matrix)
    '''
#    np.random.seed(100) # replicable
    n_index = data.shape[0]
#    n_index_shuffle = np.arange(n_index)
#    np.random.shuffle(n_index_shuffle)
    data_shuffled = data # may not need to shuffle the data...
#    data_shuffled = data.loc[n_index_shuffle, :]
    # use validation index to split; validation index: 0,1,2,3,4
    if df == True:
        if len(data.shape) > 1:
            validation_set = data_shuffled.iloc[np.int(n_index/5)*validation_index:np.int(n_index/5)*(validation_index+1), :]
            train_set = pd.concat([data_shuffled.iloc[: np.int(n_index/5)*validation_index, :], 
                                                         data_shuffled.iloc[np.int(n_index/5)*(validation_index+1):, :]]) 
        elif len(data.shape) == 1:
            validation_set = data_shuffled.iloc[np.int(n_index/5)*validation_index:np.int(n_index/5)*(validation_index+1)]
            train_set = pd.concat([data_shuffled.iloc[: np.int(n_index/5)*validation_index], 
                                                         data_shuffled.iloc[np.int(n_index/5)*(validation_index+1):]])    
    elif df == False:
        if len(data.shape) > 1:
            validation_set = data_shuffled[np.int(n_index/5)*validation_index:np.int(n_index/5)*(validation_index+1), :]
            train_set = np.concatenate([data_shuffled[: np.int(n_index/5)*validation_index, :], 
                                                         data_shuffled[np.int(n_index/5)*(validation_index+1):, :]]) 
        elif len(data.shape) == 1:
            validation_set = data_shuffled[np.int(n_index/5)*validation_index:np.int(n_index/5)*(validation_index+1)]
            train_set = np.concatenate([data_shuffled[: np.int(n_index/5)*validation_index], 
                                                         data_shuffled[np.int(n_index/5)*(validation_index+1):]])           
    
    return train_set,validation_set

X_train_validation = X[:np.int(n_index*5/6), :]  # 取前5/6作为训练集/验证集
X_test = X[np.int(n_index*5/6):, :] # 取后1/6作为测试集
X_static_train_validation = X_static[:np.int(n_index*5/6), :]  # 取前5/6作为训练集/验证集
X_static_test = X_static[np.int(n_index*5/6):, :] # 取后1/6作为测试集
Y_train_validation = Y[:np.int(n_index*5/6)]
Y_test = Y[np.int(n_index*5/6):]

## test
#train_set, validation_set = generate_cross_validation_set(df_sp_combined_train, 3)

############################################################
############################################################
### use other models as benchmarks
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.metrics import accuracy_score
from sklearn.decomposition import PCA
from sklearn.metrics import accuracy_score


############################################################
############################################################
# specify hyperparameter space for sparsely connected DNN
# define hyperparameter space
# note: [0,1,2,3,4] meaning [walk,bus,ridesharing,drive,av]

# y_vars = ['choice']
z_vars = ['hour_', 'normal_ratio', 'eld_ratio']
x0_vars = ['transfer_time1', 'avg_time1', 'full_price1', 'via_stations1', 'hourly_cnt1']
x1_vars = ['transfer_time2', 'avg_time2', 'full_price2', 'via_stations2', 'hourly_cnt2']
x2_vars = ['transfer_time3', 'avg_time3', 'full_price3', 'via_stations3', 'hourly_cnt3']
x3_vars = ['transfer_time4', 'avg_time4', 'full_price4', 'via_stations4', 'hourly_cnt4']
x4_vars = ['transfer_time5', 'avg_time5', 'full_price5', 'via_stations5', 'hourly_cnt5']

M_before_list = [0,1,2,3,4,5]
M_after_list = [0,1,2,3,4,5]
n_hidden_before_list = [10, 20, 40, 60, 80, 100]
n_hidden_after_list = [10, 20, 40, 60, 80, 100]
l1_const_list = [1e-3, 1e-5, 1e-10, 1e-20]# 8
l2_const_list = [1e-3, 1e-5, 1e-10, 1e-20]# 8
dropout_rate_list = [0.5, 0.1, 0.01, 1e-3, 1e-5] # 5
batch_normalization_list = [True, False] # 2
learning_rate_list = [0.01, 1e-3, 1e-4, 1e-5] # 5
# n_iteration_list = [500, 1000, 5000, 10000, 20000] # 5
n_iteration_list = [5000, 10000, 50000, 100000, 200000] # 5
n_mini_batch_list = [50, 100, 200, 500, 1000] # 5

# random draw...and HPO -- HyperParameter Optimization
total_sample = 50 # could change...in total it has 250 training.
sparse_dnn_dic = {}
for i in range(total_sample):
    print("------------------------")
    print("Estimate sparse connected model ", str(i))

    M_before = np.random.choice(M_before_list, size = 1)[0]
    M_after = np.random.choice(M_after_list, size = 1)[0]
    n_hidden_before = np.random.choice(n_hidden_before_list, size = 1)[0]
    n_hidden_after = np.random.choice(n_hidden_after_list, size = 1)[0]
    l1_const = np.random.choice(l1_const_list, size = 1)[0]
    l2_const = np.random.choice(l2_const_list, size = 1)[0]
    dropout_rate = np.random.choice(dropout_rate_list, size = 1)[0]
    batch_normalization = np.random.choice(batch_normalization_list, size = 1)[0]
    learning_rate = np.random.choice(learning_rate_list, size = 1)[0]
    n_iteration = np.random.choice(n_iteration_list, size = 1)[0]
    n_mini_batch = np.random.choice(n_mini_batch_list, size = 1)[0]

    # store information
    sparse_dnn_dic[i] = {}
    sparse_dnn_dic[i]['M_before'] = M_before
    sparse_dnn_dic[i]['M_after'] = M_after
    sparse_dnn_dic[i]['n_hidden_before'] = n_hidden_before
    sparse_dnn_dic[i]['n_hidden_after'] = n_hidden_after
    sparse_dnn_dic[i]['l1_const'] = l1_const
    sparse_dnn_dic[i]['l2_const'] = l2_const
    sparse_dnn_dic[i]['dropout_rate'] = dropout_rate
    sparse_dnn_dic[i]['batch_normalization'] = batch_normalization
    sparse_dnn_dic[i]['learning_rate'] = learning_rate
    sparse_dnn_dic[i]['n_iteration'] = n_iteration
    sparse_dnn_dic[i]['n_mini_batch'] = n_mini_batch
    print(sparse_dnn_dic[i])              

    for j in range(5):
        # five fold training with cross validation
        # df_sp_train,df_sp_validation = generate_cross_validation_set(df_sp_combined_train, j)

        X_train, X_val = generate_cross_validation_set(X_train_validation, j, df = False)
        X_static_train, X_static_val = generate_cross_validation_set(X_static_train_validation, j, df = False)
        Y_train, Y_val = generate_cross_validation_set(Y_train_validation, j, df = False)

        n_f = 5  # num of features
        X0_train = X_train[:,:n_f]
        X1_train = X_train[:, n_f:2*n_f]
        X2_train = X_train[:, 2*n_f:3*n_f]
        X3_train = X_train[:, 3*n_f:4*n_f]
        X4_train = X_train[:, 4*n_f:5*n_f]
        Y_train = Y_train #.reshape(-1)
        Z_train = X_static_train

        X0_val = X_val[:, :n_f]
        X1_val = X_val[:, n_f:2*n_f]
        X2_val = X_val[:, 2*n_f:3*n_f]
        X3_val = X_val[:, 3*n_f:4*n_f]
        X4_val = X_val[:, 4*n_f:5*n_f]
        Y_val = Y_val #.reshape(-1)
        Z_val = X_static_val

        X0_test = X_test[:, :n_f]
        X1_test = X_test[:, n_f:2*n_f]
        X2_test = X_test[:, 2*n_f:3*n_f]
        X3_test = X_test[:, 3*n_f:4*n_f]
        X4_test = X_test[:, 4*n_f:5*n_f]
        Y_test = Y_test #.reshape(-1)
        Z_test = X_static_test

        # one estimation here
        train_accuracy,validation_accuracy,test_accuracy,prob_cost,prob_ivt = \
                    util.dnn_alt_spec_estimation(X0_train,X1_train,X2_train,X3_train,X4_train,Y_train,Z_train,
                                            X0_val,X1_val,X2_val,X3_val,X4_val,Y_val,Z_val,
                                            X0_test,X1_test,X2_test,X3_test,X4_test,Y_test,Z_test,
                                            M_before,M_after,n_hidden_before,n_hidden_after,l1_const,l2_const,
                                            dropout_rate,batch_normalization,learning_rate,n_iteration,n_mini_batch)
        print("Training accuracy is ", train_accuracy)
        print("Validation accuracy is ", validation_accuracy)
        print("Testing accuracy is ", test_accuracy)
    
        # store information
        sparse_dnn_dic[i]['train_accuracy'+str(j)] = train_accuracy
        sparse_dnn_dic[i]['validation_accuracy'+str(j)] = validation_accuracy
        sparse_dnn_dic[i]['test_accuracy'+str(j)] = test_accuracy
        sparse_dnn_dic[i]['prob_cost'+str(j)] = prob_cost
        sparse_dnn_dic[i]['prob_ivt'+str(j)] = prob_ivt

# sparse dnn training time 
sparse_dnn_complete_time = time.time()

# export the dictionary
# import pickle
# with open('output/full_dnn_results_finer.pickle', 'wb') as full_dnn_results_finer:
#     pickle.dump(full_dnn_dic, full_dnn_results_finer, protocol=pickle.HIGHEST_PROTOCOL)
# with open('output/sparse_dnn_results_finer.pickle', 'wb') as sparse_dnn_results_finer:
#     pickle.dump(sparse_dnn_dic, sparse_dnn_results_finer, protocol=pickle.HIGHEST_PROTOCOL)
# with open('output/classifiers_accuracy.pickle', 'wb') as data:
#     pickle.dump(classifiers_accuracy, data, protocol=pickle.HIGHEST_PROTOCOL)

# print("Full DNN training time is: ", (full_dnn_complete_time - start_time)/3600) # 30 hours
# print("Sparse DNN training time is: ", (sparse_dnn_complete_time - full_dnn_complete_time)/3600) # 7 hours



#for i in range(50):
#    print("---------")
#    print(sparse_dnn_dic[i]['M_before'])
#    print(sparse_dnn_dic[i]['M_after'])
#    print(sparse_dnn_dic[i]['n_iteration'])
#    print(sparse_dnn_dic[i]['n_mini_batch'])










   



