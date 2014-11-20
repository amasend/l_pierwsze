l_pierwsze
==========
#include <iostream>

using namespace std;

int main()
{
  unsigned int i = 2,p = 2,x,z;
  bool k;

  cin >> x;
  while(i <= x)
  {
    k = true;
    for(z = 2; z < p; z++)
    {
      if(p % z == 0)
      {
        k = false;
        break;
      }
    }
    if(k)
      cout << p << " ";

    p++;
    i++;
  }
  cout << endl;
  return 0;
}
