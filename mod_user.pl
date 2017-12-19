#!/usr/bin/perl

#open(FIL, "< result.csv");
my(@lines);

foreach (`cat result.csv`) {
  if(/^User/ || /^ops0/ || /^fgi/ || /^guest/) { 
    push (@lines, $_); 
  }else{
    ($user,$info)=split(/;/,$_,2);
    $id=`id $user`;
    if($id=~m/.*\((.+)\-staff\)/) { 
      $dept=$1; 
    }elsif($id=~m/.*\(auto-ext-([a-zA-Z0-9]+)\).*/){
      $dept=$1;
    }else{
      $dept="n/a";
    }

    $name=`getent passwd $user|cut -d":" -f5`;
    $name=~s/\n//;

    push(@lines, "$user ($dept|$name);$info");
  }

}
#close(FIL);

open(FIL, "> result.csv");
foreach $line (@lines) {
  print FIL "$line";
}
close(FIL);
