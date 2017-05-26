function reducer(key,values){
  var increase = 0, decrease = 0, obs = 0;
  for (i in values){
  if (values[i]>0){
     increase = increase + 1;
     obs = obs + 1;
     }
  else if (values[i]<0){
     decrease = decrease + 1;
     obs = obs + 1;
  }
  else if (values[i]==0){
     obs = obs + 1;
     }
  else{
  
  };
  };
    return {"increase":increase/obs,"decrease":decrease/obs,"change":(increase+decrease)/obs};
};


console.log(reducer('test',[1.0,NaN,0.9,-0.8,0.9,-5,0,0,0,0,0,NaN,NaN]));
