BEGIN{
  OFS="\t"
  srand()
  for(I=0;I<100000;I++){
    X=-180+360*rand()
    Y=-90+180*rand()
    W=rand()
    print X,Y,W
  }
}
