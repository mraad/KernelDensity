BEGIN{
  OFS="\t"
  for(I=0;I<10;I++){
    X=-180+360*rand()
    Y=-90+180*rand()
    W=rand()
    print I,X,Y,W
  }
}
