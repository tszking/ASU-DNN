## Deep neural networks for choice analysis: Architectural design with alternative-specific utility functions

Shenhao Wang, Baichuan Mo, Jinhua Zhao

Whereas deep neural network (DNN) is increasingly applied to choice analysis, it is challenging to reconcile domain-specific behavioral knowledge with generic-purpose DNN, to improve DNN’s interpretability and predictive power, and to identify effective regularization methods for specific tasks. To address these challenges, this study demonstrates the use of behavioral knowledge for designing a particular DNN architecture with alternative-specific utility functions (ASU-DNN) and thereby improving both the predictive power and interpretability. Unlike a fully connected DNN (F-DNN), which computes the utility value of an alternative k by using the attributes of all the alternatives, ASU-DNN computes it by using only k’s own attributes. Theoretically, ASU- DNN can substantially reduce the estimation error of F-DNN because of its lighter architecture and sparser connectivity, although the constraint of alternative-specific utility can cause ASU- DNN to exhibit a larger approximation error. Empirically, ASU-DNN has 2-3% higher prediction accuracy than F-DNN over the whole hyperparameter space in a private dataset collected in Singapore and a public dataset available in the R mlogit package. The alternative-specific connectivity is associated with the independence of irrelevant alternative (IIA) constraint, which as a domain-knowledge-based regularization method is more effective than the most popular generic-purpose explicit and implicit regularization methods and architectural hyperparameters. ASU-DNN provides a more regular substitution pattern of travel mode choices than F-DNN does, rendering ASU-DNN more interpretable. The comparison between ASU-DNN and F-DNN also aids in testing behavioral knowledge. Our results reveal that individuals are more likely to compute utility by using an alternative’s own attributes, supporting the long-standing practice in choice modeling. Overall, this study demonstrates that behavioral knowledge can guide the architecture design of DNN, function as an effective domain-knowledge-based regularization method, and improve both the interpretability and predictive power of DNN in choice analysis. Future studies can explore the generalizability of ASU-DNN and other possibilities of using utility theory to design DNN architectures.

### Notes: this repository has incorporated the codes and two data sets. Unfortunately the Singapore data set cannot be uploaded due to the policy restriction.


