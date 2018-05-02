na.replace <- function(x, t = 0) {
  x[is.na(x)] <- t
  return(x)
}

knn_test_function2 <- function(dataset, test, distance, labels, k = 3){
  if (ncol(dataset) != ncol(test)) 
    stop("The test set and the training set have a different number of features")
  predictions <- vector('list', nrow(test))
  for (i in 1:nrow(test)) {
    if (nrow(test) == 1) {
      ordered_neighbors <- order(distance)
      if (k_type == 'percentile') {
        indices <- ordered_neighbors[1:round(length(distance) * k / 100)]
      } else {
        indices <- ordered_neighbors[1:k]
      }
      
      if (weight == 'reciprocal') {
        predictions[[i]] <- list('neighbors' = labels[indices], 
                                 'weights' = ifelse(distance[indices] == 0, 100000, 1 / distance[indices]))
      } else {
        predictions[[i]] <- list('neighbors' = labels[indices], 
                                 'weights' = 1 - distance[indices])
      }
    } else {
      ordered_neighbors <- order(distance[i, ])
      if (k_type == 'percentile') {
        indices <- ordered_neighbors[1:round(length(distance[i,]) * k / 100)]
      } else {
        indices <- ordered_neighbors[1:k]
      }
      
      if (weight == 'reciprocal') {
        predictions[[i]] <- list('neighbors' = labels[indices], 
                                 'weights' = ifelse(distance[i, indices] == 0, 100000, 1 / distance[i, indices]))
      } else {
        predictions[[i]] <- list('neighbors' = labels[indices], 
                                 'weights' = 1 - distance[i, indices])
      }
    }
  }
  return(predictions)
}

Distance_for_KNN_test <- function (test_set, train_set, VI) {
  if (ncol(test_set) != ncol(train_set)) 
    stop("The test set and the training set have a different number of features")
  distanciapost <- as.matrix(
    daisy(x = rbind(test_set, train_set), metric = 'gower', weights = VI)
  )[1:nrow(test_set), -c(1:nrow(test_set))]
  return(distanciapost)
}

mean_inlier <- function(x, w = NULL) {
  if (is.null(w)) {
    out <- boxplot(x, na.rm = TRUE, plot = FALSE)$out
    if (length(out) != 0){
      x <- x[-which(x %in% out)]
    }
    y <- mean(x, na.rm = TRUE)
  } else {
    out <- boxplot(x * w, na.rm = TRUE, plot = FALSE)$out
    if (length(out) != 0){
      z <- x * w
      x <- x[-which(z %in% out)]
      w <- w[-which(z %in% out)]
    }
    y <- weighted.mean(x, w, na.rm = TRUE)
  }
  return(y)
}

describe <- function(x) {
  y <- summary(x)
  v <- var(x, na.rm = TRUE)
  d <- c(y, v, sqrt(v))
  names(d) <- c(names(y), 'Variance', 'Std. Dev.')
  return(d)
}